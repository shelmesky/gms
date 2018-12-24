package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/log"
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

func init() {
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

func NewSocketBuffer(bufferSize int, conn *net.TCPConn) SocketBuffer {
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

func NewClient(conn *net.TCPConn) Client {
	var client Client
	client.Conn = conn
	client.ReadBuffer = NewSocketBuffer(common.READ_BUF_SIZE, conn)
	client.WriteBuffer = NewSocketBuffer(common.WRITE_BUF_SIZE, conn)
	return client
}

func GetAction(metaData []byte) int {
	actionBytes := metaData[:4]
	action := binary.LittleEndian.Uint32(actionBytes)
	return int(action)
}

func SendMessage(topicName, partitionIndex string, body []byte, bodyLen int) error {
	if len(topicName) > 0 {
		topic := topicManager.GetTopic(topicName)

		if topic != nil {
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

func batchRead(log *disklog.DiskLog, client *Client, target, length int) error {
	var bytesRead int
	var originPos int64

	segmentPos, segment, logFilePos, err := log.Search(target)
	if err != nil {
		return err
	}

	if logFilePos < 0 {
		return fmt.Errorf("cant find offset\n")
	}

	fmt.Println(logFilePos, err)

	originPos = int64(logFilePos)

	messageLength, err := segment.Log.ReadUInt32(logFilePos + 4)
	if err != nil {
		return err
	}

	logFilePos += 8
	logFilePos += int(messageLength)
	bytesRead += 8
	bytesRead += int(messageLength)

	// 减去上面读取的一条记录
	length -= 1

	readCounter := 0
	if segmentPos >= 0 && segmentPos <= log.SegmentLength()-1 {
		for {
			// 读到足够数量的消息, 退出
			if readCounter >= length {
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
				_, err := segment.Log.File.Seek(originPos, 0)
				written, err := client.Conn.ReadFrom(io.LimitReader(segment.Log.File, int64(bytesRead)))
				if written == 0 || err != nil {
					err = client.Conn.Close()
					if err != nil {
						break
					}
					break
				}

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
			}

			logFilePos += 8
			logFilePos += int(messageLength)
			bytesRead += 8
			bytesRead += int(messageLength)
			readCounter += 1
		}
	}

	return nil
}

func ReadMessage(client *Client, topicName, partitionIndex string, target, count uint32) error {
	if len(topicName) > 0 {
		topic := topicManager.GetTopic(topicName)

		if topic != nil {
			if len(partitionIndex) > 0 { // 提供了partition number
				partitionNum, err := strconv.Atoi(partitionIndex)
				if err != nil {

				}

				partition := topic.GetPartition(partitionNum)
				Log := partition.GetLog()
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

func HandleConnection(client *Client) {
	for {
		totalLength, err := client.ReadBuffer.ReadUint64()
		if err != nil {
			fmt.Println("read failed:", err)
			break
		}

		buffer := make([]byte, totalLength)

		packetReadLen := client.ReadBuffer.ReadBytes(buffer, int(totalLength))
		if packetReadLen == 0 {
			fmt.Println("connection lost")
			break
		}

		requestStartPos := 0
		requestEndPos := common.REQUEST_LEN
		requestBytes := buffer[requestStartPos:requestEndPos]
		request := common.BytesToRequest(requestBytes, common.REQUEST_LEN)

		metaDataStartPos := requestEndPos
		metaDataEndPos := metaDataStartPos + int(request.MetaDataLength)
		metaData := buffer[metaDataStartPos:metaDataEndPos]

		actionNum := GetAction(metaData)

		if actionNum == common.Write {

			fullMessageStartPos := metaDataEndPos
			fullMessageEndPos := fullMessageStartPos + int(request.BodyLength)
			fullMessage := buffer[fullMessageStartPos:fullMessageEndPos]

			fmt.Printf("receive [%d] request: %v, %v\n", len(requestBytes), requestBytes, request)
			fmt.Printf("receive [%d] metadata %s, %v\n", len(metaData), string(metaData), metaData)

			/*
				///////////////////////////////////////////////////////////////////
				messageHeadStartPos := metaDataEndPos
				messageHeadEndPos := messageHeadStartPos + common.MESSAGE_LEN
				messageHeadBytes := buffer[messageHeadStartPos:messageHeadEndPos]
				messageHead := common.BytesToMessage(messageHeadBytes)

				messageKeyStartPos := messageHeadEndPos
				messageKeyEndPos := messageKeyStartPos + int(messageHead.KeyLength)
				messageKey := buffer[messageKeyStartPos:messageKeyEndPos]

				messageValueStartPos := messageKeyEndPos
				messageValueEndPos := messageValueStartPos + int(messageHead.ValueLength)
				messageValue := buffer[messageValueStartPos:messageValueEndPos]

				fmt.Printf("***********************************************\n")
				fmt.Printf("receive [%d] full message: %v\n", len(fullMessage), fullMessage)
				fmt.Printf("receive [%d] message head: %v, %v\n", len(messageHeadBytes), messageHeadBytes, messageHead)
				fmt.Printf("receive [%d] message key: %v, %s\n", len(messageKey), messageKey, string(messageKey))
				fmt.Printf("receive [%d] message value: %v, %s\n", len(messageValue), messageValue, string(messageValue))
				fmt.Printf("***********************************************\n\n")
				///////////////////////////////////////////////////////////////////
			*/

			action := common.BytesToWriteMessageAction(metaData)
			topicName := string(bytes.Trim(action.TopicName[:], "\x00"))
			partitionNum := string(bytes.Trim(action.PartitionNumber[:], "\x00"))

			err = SendMessage(topicName, partitionNum, fullMessage, len(fullMessage))
			if err != nil {
				fmt.Printf("send message to %s failed: %s\n", topicName, err)
			}
		}

		if actionNum == common.Read {
			action := common.BytesToReadMessageAction(metaData)
			topicName := string(bytes.Trim(action.TopicName[:], "\x00"))
			partitionNum := string(bytes.Trim(action.PartitionNumber[:], "\x00"))
			targetOffset := action.TargetOffset
			count := action.Count
			err = ReadMessage(client, topicName, partitionNum, targetOffset, count)
			if err != nil {
				fmt.Printf("read message from %s-%s failed: %s", topicName, partitionNum, err.Error())
			}
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
		client := NewClient(conn)
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

	StartServer(listen)
}
