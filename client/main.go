package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"os"
	"strconv"
	"time"
)

var (
	action        = flag.String("action", "", "action need to do, default is empty")
	serverAddress = flag.String("address", "127.0.0.1", "ip address of server")
	serverPort    = flag.Int("port", 50051, "tcp port of server")
	syncTopic     = flag.String("sync-topic", "all", "let controller send SET_SYNC to follower")
)

func CRC32(data []byte) {

}

/*
传入key和value创建消息
返回消息的bytes和消息的总长度(head+key+value)
*/
func NewBody(key, value []byte) ([]byte, uint64) {
	var message common.WriteMessageType
	message.CRC32 = 99
	message.Magic = 98
	message.Attributes = 97
	message.KeyLength = uint64(len(key))
	message.ValueLength = uint64(len(value))

	messageBytesLen := common.WRITE_MESSAGE_LEN

	messageTotalLength := messageBytesLen + int(message.KeyLength) + int(message.ValueLength)
	message.Length = uint64(messageTotalLength)

	messageBytes := common.MessageToBytes(&message)

	return messageBytes, message.Length
}

func NewWriteMessageMeta(topicName, partitionNum string, Ack int8) ([]byte, uint32) {
	meta := common.NewWriteMessageAction(topicName, partitionNum, Ack)
	return meta, uint32(len(meta))
}

func NewMessage(bodyKey, bodyValue []byte, netBuffer *net.Buffers) uint64 {

	var messageHead []byte
	var bodyLen uint64

	messageHead, bodyLen = NewBody(bodyKey, bodyValue)

	*netBuffer = append(*netBuffer, messageHead)
	*netBuffer = append(*netBuffer, bodyKey)
	*netBuffer = append(*netBuffer, bodyValue)

	return bodyLen
}

func WriteMessage(conn *net.TCPConn, topicName, partitionNum string, ack int8) {
	var MetaData []byte
	var netBufferBody net.Buffers
	var netBufferReq net.Buffers
	var request common.RequestHeader

	totalBodyLength := uint64(0)

	request.Version = 1001
	request.Sequence = 1

	MetaData, request.MetaDataLength = NewWriteMessageMeta(topicName, partitionNum, ack)

	// 循环增加消息体
	for i := 0; i < 1; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		totalBodyLength += NewMessage(key, value, &netBufferBody)
	}

	request.BodyLength = uint32(totalBodyLength)

	// 本次请求的总长度
	request.TotalLength = 8 + common.REQUEST_LEN + uint64(request.MetaDataLength) + totalBodyLength
	totalLengthBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(totalLengthBytes, request.TotalLength)

	// 请求结构体
	requestBytes := common.RequestToBytes(&request)

	netBufferReq = append(netBufferReq, totalLengthBytes)
	netBufferReq = append(netBufferReq, requestBytes)
	netBufferReq = append(netBufferReq, MetaData)

	requestTotalLen := uint32(0)
	requestTotalLen = 8 + common.REQUEST_LEN + request.MetaDataLength

	// 发送总长度, 请求结构体, metadata
	n, err := netBufferReq.WriteTo(conn)
	if err != nil {
		log.Errorln("netbuffer.WriteTo failed:", err)
		return
	}

	if n != int64(requestTotalLen) {
		log.Errorf("written length [%d] is too small than: [%d]\n", n, requestTotalLen)
		return
	} else {
		log.Printf("written [totalLength, request, metadata] %d bytes\n", n)
	}

	// 发送N个消息
	n, err = netBufferBody.WriteTo(conn)
	if err != nil {
		log.Errorln("netbuffer.WriteTo failed:", err)
		return
	}

	if uint64(n) != totalBodyLength {
		log.Errorf("written length [%d] is too small than: [%d]\n", n, totalBodyLength)
		return
	} else {
		log.Printf("written [body] %d bytes\n", n)
	}

	// 如果ack等于0,则客户端发送完成读写请求就直接退出，不会等待服务器的响应
	if ack == 0 {
		return
	}

	responseHeader, err := common.ReadResponseHeader(conn)
	if err != nil {
		log.Errorln("read response header failed:", err)
		return
	}

	log.Println("read reponse:", responseHeader.Code, string(responseHeader.Message[:]))
}

func CreateTopic(conn *net.TCPConn, topicName string, partitionCount, replicaCount uint32, inSyncReplicas uint32) {
	//生成meta数据
	metaData := common.NewCreateTopicAction(topicName, partitionCount, replicaCount, inSyncReplicas)
	// 生成body数据
	bodyData := []byte{}

	// 把request、meta、body数据合并保存在net.Buffers结构中
	netBuffer, totalLength := common.NewRequest(metaData, bodyData)

	// 使用writev系用调用发送数据
	n, err := netBuffer.WriteTo(conn)
	if uint64(n) != totalLength || err != nil {
		log.Errorf("Writev failed, total length: %d, data written: %d, error: %s\n", totalLength, n, err)
		if err = conn.Close(); err != nil {
			log.Errorln("close socket failed:", err)
		}
	}

	// 读取服务器返回的response
	responseHeader, err := common.ReadResponseHeader(conn)
	if err != nil {
		log.Errorln("read response header failed:", err)
	} else {
		log.Println("read response:", responseHeader.Code, string(responseHeader.Message[:]))
	}
}

func ReadMessage(conn *net.TCPConn, topicName, partitionNum string, targetOffset, count uint32) {
	var request common.RequestHeader
	var metaData []byte
	var netBuffer net.Buffers

	request.Version = 1001
	request.Sequence = 2

	metaData = common.NewReadMessageAction(topicName, partitionNum, targetOffset, count)
	metaDataLen := uint32(len(metaData))

	request.MetaDataLength = metaDataLen
	request.TotalLength = common.REQUEST_LEN + uint64(request.MetaDataLength)
	request.BodyLength = 0

	requestBytes := common.RequestToBytes(&request)

	totalLengthBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(totalLengthBytes, request.TotalLength)

	netBuffer = append(netBuffer, totalLengthBytes)
	netBuffer = append(netBuffer, requestBytes)
	netBuffer = append(netBuffer, metaData)

	requestTotalLen := uint32(0)
	requestTotalLen = 8 + common.REQUEST_LEN + request.MetaDataLength

	// 发送总长度, 请求结构体, metadata
	n, err := netBuffer.WriteTo(conn)
	if n != int64(requestTotalLen) {
		log.Errorf("written length [%d] is too small than: [%d]\n", n, requestTotalLen)
		return
	} else {
		log.Printf("written %d bytes\n", n)
	}

	if err != nil {
		log.Errorln("netbuffer.WriteTo failed:", err)
		return
	}

	if n != int64(requestTotalLen) {
		log.Errorf("written length [%d] is too small than: [%d]\n", n, requestTotalLen)
		return
	} else {
		log.Printf("written %d bytes\n", n)
	}

	for {
		offsetBuf := make([]byte, 4)
		lengthBuf := make([]byte, 4)

		readN, err := io.ReadFull(conn, offsetBuf)
		if readN == 0 || err != nil {
			break
		}

		offset := binary.LittleEndian.Uint32(offsetBuf)

		readN, err = io.ReadFull(conn, lengthBuf)
		if readN == 0 || err != nil {
			break
		}

		length := binary.LittleEndian.Uint32(lengthBuf)

		log.Printf("offset: %d, length: %d\n", offset, length)

		bodyBuf := make([]byte, length)

		readN, err = io.ReadFull(conn, bodyBuf)
		if readN == 0 || err != nil {
			break
		}

		body := common.BytesToMessage(bodyBuf)
		log.Println("body: ", body)

		key := bodyBuf[common.WRITE_MESSAGE_LEN : common.WRITE_MESSAGE_LEN+body.KeyLength]
		value := bodyBuf[common.WRITE_MESSAGE_LEN+body.KeyLength : common.WRITE_MESSAGE_LEN+body.KeyLength+body.ValueLength]
		log.Println("key: ", string(key))
		log.Println("value: ", string(value))
	}
}

func StartSyncTopic(conn *net.TCPConn, topicName string) {
	//生成meta数据
	metaData := common.NewStartSyncTopicAction(topicName)
	// 生成body数据
	bodyData := []byte{}

	// 把request、meta、body数据合并保存在net.Buffers结构中
	netBuffer, totalLength := common.NewRequest(metaData, bodyData)

	// 使用writev系用调用发送数据
	n, err := netBuffer.WriteTo(conn)
	if uint64(n) != totalLength || err != nil {
		log.Errorf("Writev failed, total length: %d, data written: %d, error: %s\n", totalLength, n, err)
		if err = conn.Close(); err != nil {
			log.Errorln("close socket failed:", err)
		}
	}

	// 读取服务器返回的response
	responseHeader, err := common.ReadResponseHeader(conn)
	if err != nil {
		log.Errorln("read response header failed:", err)
	} else {
		log.Println("read response:", responseHeader.Code, string(responseHeader.Message[:]))
	}
}

func main() {
	flag.Parse()

	port := strconv.Itoa(*serverPort)
	target := *serverAddress + ":" + port
	conn, err := net.DialTimeout("tcp", target, time.Second*2)

	if err != nil {
		log.Errorln("dial failed:", err)
		return
	}

	start := time.Now().UnixNano() / int64(time.Millisecond)

	tcpConn := conn.(*net.TCPConn)

	if *action == "" {
		log.Errorln("please specify action!")
		os.Exit(1)
	}

	if *action == "writeMessage" {
		topicName := "testtopic"
		partitionNum := "0"
		ack := -1
		WriteMessage(tcpConn, topicName, partitionNum, int8(ack))
	} else if *action == "readMessage" {
		ReadMessage(tcpConn, "testtopic", "0", 1, 5)
	} else if *action == "createTopic" {
		CreateTopic(tcpConn, "testtopic", 3, 3, 2)
	} else if *action == "startSync" {
		StartSyncTopic(tcpConn, *syncTopic)
	} else {
		log.Errorln("action is not support!")
		os.Exit(1)
	}

	end := time.Now().UnixNano() / int64(time.Millisecond)

	log.Warningf("duration [%v] milliseconds.\n", end-start)
}
