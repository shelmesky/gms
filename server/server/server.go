package server

import (
	"bytes"
	"encoding/binary"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/controller"
	"github.com/shelmesky/gms/server/node"
	"github.com/shelmesky/gms/server/rpc"
	"github.com/shelmesky/gms/server/topics"
	"github.com/shelmesky/gms/server/utils"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"strconv"
)

func getAction(metaData []byte) int {
	actionBytes := metaData[:4]
	action := binary.LittleEndian.Uint32(actionBytes)
	return int(action)
}

// 根据topic名字获取内存中Topic对象并写入数据
func writeMessage(topicName, partitionIndex string, body []byte, bodyLen int) error {
	if len(topicName) > 0 {
		// 获取Topic对象
		topic := topics.TopicManager.GetTopic(topicName)

		targetPartition, err := strconv.Atoi(partitionIndex)
		if err != nil {
			return utils.ParameterPartitionIndexMissed
		}
		partition := topic.GetPartition(targetPartition)
		if partition == nil {
			return utils.PartitionNotExist
		}

		if topic != nil {
			// 在名字对应的Topic中写入消息体
			err := topic.AppendMessage(partitionIndex, body, bodyLen)
			if err != nil {
				return err
			}

			// Leader写消息到磁盘完毕后， 等待Follower同步完成
			err = rpc.GlobalFollowerManager.WaitOffset(topicName, targetPartition, partition.GetCurrentOffset())
			// 如果发生错误， 则producer应该重新发送
			if err != nil {
				return err
			}

			return nil

		} else {
			return utils.ServerError
		}

	}

	return utils.ParameterTopicMissed
}

// 解析客户端的请求， 发挥request结构， action结构， 和body数据
func ParseRequest(data []byte) (*common.RequestHeader, interface{}, interface{}) {
	// 读取固定长读的Request头部
	requestStartPos := 0
	requestEndPos := common.REQUEST_LEN
	requestBytes := data[requestStartPos:requestEndPos]
	request := common.BytesToRequest(requestBytes, common.REQUEST_LEN)

	// 读取不定长度的Meta数据
	metaDataStartPos := requestEndPos
	metaDataEndPos := metaDataStartPos + int(request.MetaDataLength)
	metaData := data[metaDataStartPos:metaDataEndPos]

	actionNum := getAction(metaData)

	if actionNum == common.Write {
		fullMessageStartPos := metaDataEndPos
		fullMessageEndPos := fullMessageStartPos + int(request.BodyLength)
		fullMessage := data[fullMessageStartPos:fullMessageEndPos]

		writeAction := common.BytesToWriteMessageAction(metaData)

		return request, writeAction, fullMessage

	} else if actionNum == common.Read {
		readAction := common.BytesToReadMessageAction(metaData)

		return request, readAction, nil

	} else if actionNum == common.CreateTopic {
		createTopicAction := common.BytesToCreateTopicAction(metaData)
		return request, createTopicAction, nil

	} else if actionNum == common.StartSyncTopic {
		startSyncAction := common.BytesToStartSyncTopicAction(metaData)
		return request, startSyncAction, nil
	}

	return request, nil, nil
}

// 处理客户端的写请求
func handleWriteAction(client *common.Client, request *common.RequestHeader, action *common.WriteMessageAction, body []byte) {
	var response *common.Response

	topicName := string(bytes.Trim(action.TopicName[:], "\x00"))
	partitionNum := string(bytes.Trim(action.PartitionNumber[:], "\x00"))

	// 写入消息
	err := writeMessage(topicName, partitionNum, body, len(body))
	if err != nil {
		log.Errorf("send message to %s failed: %s\n", topicName, err)
		response = common.NewResponse(1, "FAILED", []byte{})
	} else {
		response = common.NewResponse(0, "OK", []byte{})
	}

	err = response.WriteTo(client.Conn)
	if err != nil {
		log.Printf("handleWriteAction() write data to [%s] failed: %v\n", client.Conn.RemoteAddr(), err)
		return
	}
}

// 处理客户端的读请求
func handleReadAction(client *common.Client, request *common.RequestHeader, action *common.ReadMessageAction,
	body interface{}) {
	// 获取topic名字
	topicName := string(bytes.Trim(action.TopicName[:], "\x00"))
	// 获取partition序号
	partitionNum := string(bytes.Trim(action.PartitionNumber[:], "\x00"))
	// 获取client希望开始读取的offset
	targetOffset := action.TargetOffset
	// 获取client希望读取的消息数量
	count := action.Count
	// 读取消息
	err := topics.ReadMessage(client, topicName, partitionNum, targetOffset, count)
	if err != nil {
		log.Errorf("read message from %s-%s failed: %s", topicName, partitionNum, err.Error())
	}
}

// 处理客户端的创建topic请求
func handleCreateTopicAction(client *common.Client, request *common.RequestHeader, action *common.CreateTopicAction,
	body interface{}) {

	var response *common.Response

	// 获取topic name
	topicName := string(bytes.Trim(action.TopicName[:], "\x00"))
	partitionCount := action.PartitionCount
	replicaCount := action.ReplicaCount

	err := CreateTopicOnEtcd(topicName, partitionCount, replicaCount)
	if err != nil {
		response = common.NewResponse(1, "FAILED", []byte{})
	}
	response = common.NewResponse(0, "OK", []byte{})

	err = response.WriteTo(client.Conn)
	if err != nil {
		log.Printf("handleCreateTopicAction() write data to [%s] failed: %v\n", client.Conn.RemoteAddr(), err)
	}

	log.Println("")
}

func handleStartSyncAction(client *common.Client, request *common.RequestHeader, action *common.StartSyncTopicAction) {
	var response *common.Response

	topicName := string(bytes.Trim(action.TopicName[:], "\x00"))
	processTopicErr := controller.ProcessTopics(topicName)

	if processTopicErr != nil {
		response = common.NewResponse(1, "FAILED", []byte{})
	}
	response = common.NewResponse(0, "OK", []byte{})

	err := response.WriteTo(client.Conn)
	if err != nil {
		log.Printf("handleStartSyncAction() write data to [%s] failed: %v\n", client.Conn.RemoteAddr(), err)
		return
	}

	if processTopicErr != nil {
		log.Printf("server start sync for topic [%s] failed: %s\n", topicName, processTopicErr.Error())
	} else {
		log.Printf("server start sync for topic [%s]\n", topicName)
	}
}

func HandleConnection(client *common.Client) {
	for {
		// 获取totalLength即整个数据包的总长度
		totalLength, err := client.ReadBuffer.ReadUint64()
		if err != nil {
			log.Errorln("read failed:", err)
			break
		}

		// 实际的总长度应减去第一个64位字段
		totalLength = totalLength - 8

		// 分配内存
		// TODO: 使用内存池
		buffer := make([]byte, totalLength)

		// 读取所有数据包
		packetReadLen := client.ReadBuffer.ReadBytes(buffer, int(totalLength))
		if packetReadLen == 0 {
			log.Errorln("connection lost")
			break
		}

		request, actionInterface, bodyInterface := ParseRequest(buffer)
		switch action := actionInterface.(type) {

		case *common.WriteMessageAction:
			body := bodyInterface.([]byte)
			handleWriteAction(client, request, action, body)

		case *common.ReadMessageAction:
			handleReadAction(client, request, action, nil)

		case *common.CreateTopicAction:
			go handleCreateTopicAction(client, request, action, nil)

		case *common.StartSyncTopicAction:
			go handleStartSyncAction(client, request, action)
		}
	}

	log.Println("close connection:", client.Conn.Close())
}

func StartServer(listener *net.TCPListener) {
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Errorln("Accept() failed:", err)
			continue
		}

		sockFile, err := conn.File()
		if err != nil {
			panic(err.Error())
		}
		client := common.NewClient(conn)
		client.SockFD = int(sockFile.Fd())

		go HandleConnection(&client)
	}
}

func RunRPC(address string, port int) {
	addr := &net.TCPAddr{net.ParseIP(address), port, ""}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Println("RPC Server listen at", addr)

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Errorln("Accept() failed:", err)
			continue
		}

		go rpc.RPCHandleConnection(conn)
	}
}

func Run(address string, port int) {
	addr := &net.TCPAddr{net.ParseIP(address), port, ""}
	listen, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Println("GMS server listen at", addr)

	StartNode()       // 注册节点
	StartController() // 注册控制器

	topics.Init()
	StartServer(listen) // 启动服务
}

func StartController() {
	controller.Start()
}

func StartNode() {
	_, err := node.Start()
	if err != nil {
		log.Errorln("start node failed:", err)
		os.Exit(1)
	}
}
