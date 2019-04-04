package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/controller"
	"github.com/shelmesky/gms/server/log"
	"github.com/shelmesky/gms/server/node"
	"github.com/shelmesky/gms/server/topics"
	"github.com/shelmesky/gms/server/utils"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strconv"
)

var (
	topicManager *topics.Topics
)

func Init() {
	if err := os.Chdir("./data"); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var t topics.Topics
	err := t.Init()
	if err != nil {
		panic(err.Error())
	}

	topicManager = &t
}

type SocketBuffer struct {
	conn   *net.TCPConn
	buffer []byte
	size   int
	in     int
	out    int
}

func newSocketBuffer(bufferSize int, conn *net.TCPConn) SocketBuffer {
	var socketBuffer SocketBuffer
	socketBuffer.buffer = make([]byte, bufferSize, bufferSize)
	socketBuffer.size = bufferSize
	socketBuffer.in = 0
	socketBuffer.out = 0
	socketBuffer.conn = conn
	return socketBuffer
}

func (b *SocketBuffer) ReadFromSocket() int {
	remain := b.Remain()
	buffer := make([]byte, remain)
	n, err := b.conn.Read(buffer)
	if err == io.EOF {
		return 0
	}
	if err != nil {
		return 0
	}
	fmt.Printf("read %d bytes from socket\n", n)
	b.WriteBytes(buffer[:n], n)

	return n
}

func (b *SocketBuffer) ReadBytes(buffer []byte, size int) int {
	originSize := size
	n := 0
	for {
		length := 0
		size = int(math.Min(float64(size), float64(b.in-b.out)))
		length = int(math.Min(float64(size), float64(b.size-(b.out&(b.size-1)))))
		start := b.out & (b.size - 1)
		// TODO: 性能优化
		// 当ring buffer中的数据长度满足需求, 且是连续的(没有回绕)
		// 应该直接返回ring buffer的slice切片, 而不是copy
		copy(buffer, b.buffer[start:start+length])
		copy(buffer[length:], b.buffer[0:size-length])
		b.out += size
		originSize -= size
		n += size
		if originSize > 0 {
			size = originSize
			readN := b.ReadFromSocket()
			if readN == 0 {
				break
			}
		} else {
			break
		}
	}
	return n
}

func (b *SocketBuffer) WriteBytes(buffer []byte, size int) int {
	fmt.Printf("write %d bytes to ring buffer\n", len(buffer))
	if size > b.size {
		return 0
	}
	length := 0
	size = int(math.Min(float64(size), float64(b.size-b.in+b.out)))
	length = int(math.Min(float64(size), float64(b.size-(b.in&(b.size-1)))))
	start := b.in & (b.size - 1)
	copy(b.buffer[start:start+length], buffer)
	if (size - length) >= 1 {
		copy(b.buffer[size-length-1:], buffer[length:])
	}
	b.in += size
	return size
}

func (b *SocketBuffer) ReadUint32() (uint32, error) {
	buffer := make([]byte, 4)
	length := b.ReadBytes(buffer, 4)
	if length != 4 {
		return 0, fmt.Errorf("read zero length")
	}

	return binary.LittleEndian.Uint32(buffer), nil
}

func (b *SocketBuffer) ReadUint64() (uint64, error) {
	buffer := make([]byte, 8)
	length := b.ReadBytes(buffer, 8)
	if length != 8 {
		return 0, fmt.Errorf("read zero length")
	}

	return binary.LittleEndian.Uint64(buffer), nil
}

func (b *SocketBuffer) Remain() int {
	return b.size - (b.in - b.out)
}

type Client struct {
	Conn        *net.TCPConn
	SockFD      int
	Alive       bool
	ReadBuffer  SocketBuffer
	WriteBuffer SocketBuffer
}

func newClient(conn *net.TCPConn) Client {
	var client Client
	client.Conn = conn
	client.ReadBuffer = newSocketBuffer(common.READ_BUF_SIZE, conn)
	client.WriteBuffer = newSocketBuffer(common.WRITE_BUF_SIZE, conn)
	return client
}

func getAction(metaData []byte) int {
	actionBytes := metaData[:4]
	action := binary.LittleEndian.Uint32(actionBytes)
	return int(action)
}

// 根据topic名字获取内存中Topic对象并写入数据
func writeMessage(topicName, partitionIndex string, body []byte, bodyLen int) error {
	if len(topicName) > 0 {
		// 获取Topic对象
		topic := topicManager.GetTopic(topicName)

		if topic != nil {
			// 在名字对应的Topic中写入消息体
			err := topic.AppendMessage(partitionIndex, body, bodyLen)
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

func sendFileToSocket(segment *disklog.LogIndexSegment, client *Client, pos, length int64) error {
	_, err := segment.Log.File.Seek(pos, 0)
	written, err := client.Conn.ReadFrom(io.LimitReader(segment.Log.File, length))
	if written == 0 || err != nil {
		err = client.Conn.Close()
		if err != nil {
			return utils.CloseConnError
		}
		return utils.WrittenNotEnoughError
	}

	return nil
}

/*
从Log对象中批量读取消息
log: 管理磁盘日志的对象
client: 客户端连接对象
target: 开始读取的offset
length: 希望读取几个消息
*/
func batchRead(log *disklog.DiskLog, client *Client, target, length int) error {
	var bytesRead int
	var originPos int64

	// 根据target参数在Log对象中搜索是否存在对应的offset
	// 获取segment即对应的文件段对象
	// segmentPos即文件段对象在所有segment list中的索引
	// logFilePos即target offset在对应segment开始读取的位置
	segmentPos, segment, logFilePos, err := log.Search(target)
	if err != nil {
		return err
	}

	// 如果logFilePos小于0,说明在index文件中找到target offset
	if logFilePos < 0 {
		return fmt.Errorf("cant find offset\n")
	}

	// 获取第一条消息的长度
	messageLength, err := segment.Log.ReadUInt32(logFilePos + 4)
	if err != nil {
		return err
	}

	// 发送数据
	err = sendFileToSocket(segment, client, int64(logFilePos), int64(messageLength)+8)
	if err != nil {
		return err
	}

	// 将消息指针向前移动4+4+MessageLength
	logFilePos += 8
	logFilePos += int(messageLength)

	// 保存此时第一次读取到的POS位置
	// 进入循环之前保存
	originPos = int64(logFilePos)

	// 减去上面读取的一条记录
	length -= 1

	readCounter := 0

	/*
		logFilePos: 跟踪当前log文件读取的位置
		originPos: 用于sendfile时传递开始的位置
		bytesRead: 用于从start offset到文件末尾或者读够了数量时，记录总共读取了多少数据.
	*/

	for {
		// 读到足够数量的消息, 退出
		if readCounter >= length {
			err = sendFileToSocket(segment, client, originPos, int64(bytesRead))
			if err != nil {
				return err
			}
			break
		}

		// 消息的长度
		messageLength, err := segment.Log.ReadUInt32(logFilePos + 4)
		if err != nil {
			return err
		}

		// 说明读到了log文件末尾
		// 切换下一个segment并从文件开始处读
		if messageLength == 0 {
			// 将当前segment的内容发送到client
			err = sendFileToSocket(segment, client, originPos, int64(bytesRead))
			if err != nil {
				return err
			}

			// 重置originPos，因为切换了新的文件
			originPos = 0
			bytesRead = 0

			// 切换到下个segment继续读
			segmentPos += 1
			segment, err = log.GetSegment(segmentPos)
			// 如果返回错误,说明已经读取完所有的segment
			if err != nil {
				return err
			}
			logFilePos = 0
			continue
		} else {
			//未读到文件尾，继续读取

			// 增加文件读取位置的指针
			logFilePos += 8
			logFilePos += int(messageLength)

			bytesRead += 8
			bytesRead += int(messageLength)

			// 读取的数量自增1
			readCounter += 1
		}
	}

	return nil
}

func readMessage(client *Client, topicName, partitionIndex string, target, count uint32) error {
	// 必须提供长度大于0的topic名字
	if len(topicName) > 0 {
		// 根据名字获得topic对象
		topic := topicManager.GetTopic(topicName)

		// 如果根据topic名字能找到Topic对象
		if topic != nil {
			// 提供了partition number
			if len(partitionIndex) > 0 {
				// partition序号
				partitionNum, err := strconv.Atoi(partitionIndex)
				if err != nil {

				}

				// 根据partition序号找到Partition
				partition := topic.GetPartition(partitionNum)
				// Partition的Log对象
				Log := partition.GetLog()
				// 使用Log对象批量读取消息
				err = batchRead(Log, client, int(target), int(count))

				if err != nil {
					return err
				}
				return nil
			} else {
				// TODO: 未提供partition number
			}

		} else {
			return utils.ServerError
		}

	}

	return utils.ParameterTopicMissed
}

// 解析客户端的请求， 发挥request结构， action结构， 和body数据
func parseRequest(data []byte) (*common.Request, interface{}, interface{}) {
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
	}

	return request, nil, nil
}

// 处理客户端的写请求
func handleWriteAction(request *common.Request, action *common.WriteMessageAction, body []byte) {
	topicName := string(bytes.Trim(action.TopicName[:], "\x00"))
	partitionNum := string(bytes.Trim(action.PartitionNumber[:], "\x00"))

	// 写入消息
	err := writeMessage(topicName, partitionNum, body, len(body))
	if err != nil {
		fmt.Printf("send message to %s failed: %s\n", topicName, err)
	}
}

// 处理客户端的读请求
func handleReadAction(client *Client, request *common.Request, action *common.ReadMessageAction, body interface{}) {
	// 获取topic名字
	topicName := string(bytes.Trim(action.TopicName[:], "\x00"))
	// 获取partition序号
	partitionNum := string(bytes.Trim(action.PartitionNumber[:], "\x00"))
	// 获取client希望开始读取的offset
	targetOffset := action.TargetOffset
	// 获取client希望读取的消息数量
	count := action.Count
	// 读取消息
	err := readMessage(client, topicName, partitionNum, targetOffset, count)
	if err != nil {
		fmt.Printf("read message from %s-%s failed: %s", topicName, partitionNum, err.Error())
	}
}

// 处理客户端的创建topic请求
func handleCreateTopicAction(request *common.Request, action *common.CreateTopicAction, body interface{}) {
	// 获取topic name
	topicName := string(bytes.Trim(action.TopicName[:], "\x00"))
	partitionCount := action.PartitionCount
	replicaCount := action.ReplicaCount

	err := CreateTopicOnEtcd(topicName, partitionCount, replicaCount)
	if err != nil {

	}

	fmt.Println("got create topic request:", topicName, partitionCount, replicaCount)
}

func HandleConnection(client *Client) {
	for {
		// 获取totalLength即整个数据包的总长度
		totalLength, err := client.ReadBuffer.ReadUint64()
		if err != nil {
			fmt.Println("read failed:", err)
			break
		}

		// 分配内存
		// TODO: 使用内存池
		buffer := make([]byte, totalLength)

		// 读取所有数据包
		packetReadLen := client.ReadBuffer.ReadBytes(buffer, int(totalLength))
		if packetReadLen == 0 {
			fmt.Println("connection lost")
			break
		}

		request, actionInterface, bodyInterface := parseRequest(buffer)
		switch action := actionInterface.(type) {

		case *common.WriteMessageAction:
			body := bodyInterface.([]byte)
			handleWriteAction(request, action, body)

		case *common.ReadMessageAction:
			handleReadAction(client, request, action, nil)

		case *common.CreateTopicAction:
			handleCreateTopicAction(request, action, nil)
		}
	}

	fmt.Println("close connection:", client.Conn.Close())
}

func StartServer(listener *net.TCPListener) {
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			fmt.Println("Accept() failed:", err)
			continue
		}

		sockFile, err := conn.File()
		if err != nil {
			panic(err.Error())
		}
		client := newClient(conn)
		client.SockFD = int(sockFile.Fd())

		go HandleConnection(&client)
	}
}

func Run(address string, port int) {
	addr := &net.TCPAddr{net.ParseIP(address), port, ""}
	listen, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	StartNode()       // 注册节点
	StartController() // 注册控制器

	Init()              // 初始化数据目录
	StartServer(listen) // 启动服务
}

func StartController() {
	controller.Start()
}

func StartNode() {
	_, err := node.Start()
	if err != nil {
		fmt.Println("start node failed:", err)
		os.Exit(1)
	}
}
