package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"github.com/sirupsen/logrus"
	"net"
	"os"
)

var (
	action        = flag.String("action", "", "action need to do, default is empty")
	serverAddress = flag.String("address", "127.0.0.1", "ip address of server")
	serverPort    = flag.Int("port", 50051, "tcp port of server")
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

func NewWriteMessageMeta(topicName, partitionNum string) ([]byte, uint32) {
	meta := common.NewWriteMessageAction(topicName, partitionNum)
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

func WriteMessage(conn *net.TCPConn) {
	topicName := "mytopic"
	partitionNum := "0"

	var MetaData []byte
	var netBufferBody net.Buffers
	var netBufferReq net.Buffers
	var request common.Request

	totalBodyLength := uint64(0)

	request.Version = 1001
	request.Sequence = 1

	MetaData, request.MetaDataLength = NewWriteMessageMeta(topicName, partitionNum)

	// 循环增加消息体
	for i := 0; i < 2; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		totalBodyLength += NewMessage(key, value, &netBufferBody)
	}

	request.BodyLength = uint32(totalBodyLength)

	// 本次请求的总长度
	request.TotalLength = common.REQUEST_LEN + uint64(request.MetaDataLength) + totalBodyLength
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
		fmt.Println("netbuffer.WriteTo failed:", err)
		return
	}

	if n != int64(requestTotalLen) {
		fmt.Printf("written length [%d] is too small than: [%d]\n", n, requestTotalLen)
		return
	} else {
		fmt.Printf("written %d bytes\n", n)
	}

	// 发送N个消息
	n, err = netBufferBody.WriteTo(conn)
	if err != nil {
		fmt.Println("netbuffer.WriteTo failed:", err)
		return
	}
	if uint32(n) != requestTotalLen {
		fmt.Printf("written length [%d] is too small than: [%d]\n", n, totalBodyLength)
		return
	} else {
		fmt.Printf("written %d bytes\n", n)
	}
}

func NewRequest(metaData, bodyData []byte) (net.Buffers, uint64) {
	var request common.Request
	var netBuffer net.Buffers

	// 设置request的版本号和请求序号
	request.Version = 1001
	request.Sequence = 2

	metaDataLength := len(metaData)
	bodyLength := len(bodyData)

	// 设置request中的各种长度属性
	request.BodyLength = uint32(bodyLength)                                                    // body数据长度
	request.MetaDataLength = uint32(metaDataLength)                                            // meta数据长度
	request.TotalLength = 8 + common.REQUEST_LEN + uint64(metaDataLength) + uint64(bodyLength) // 总长度

	// 将request结构体转换为[]byte
	requestBytes := common.RequestToBytes(&request)

	// 写入总长度到一个8字节的数组中
	totalLengthBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(totalLengthBytes, request.TotalLength)

	// 想net.Buffers中追加字节数组
	netBuffer = append(netBuffer, totalLengthBytes) // 数据包总长度数据
	netBuffer = append(netBuffer, requestBytes)     // request结构数据
	netBuffer = append(netBuffer, metaData)         // meta结构数据
	if bodyLength > 0 {
		netBuffer = append(netBuffer, bodyData) // body数据
	}

	// 返回net.Buffers和总长度
	return netBuffer, request.TotalLength
}

func CreateTopic(conn *net.TCPConn, topicName string, partitionCount, replicaCount uint32) {
	//生成meta数据
	metaData := common.NewCreateTopicAction(topicName, partitionCount, replicaCount)
	// 生成body数据
	bodyData := []byte{}

	// 把request、meta、body数据合并保存在net.Buffers结构中
	netBuffer, totalLength := NewRequest(metaData, bodyData)

	// 使用writev系用调用发送数据
	n, err := netBuffer.WriteTo(conn)
	if uint64(n) != totalLength || err != nil {
		logrus.Printf("Writev failed, total length: %d, data written: %d, error: %s\n", totalLength, n, err)
		if err = conn.Close(); err != nil {
			logrus.Println("close socket failed:", err)
		}
	}
}

func ReadMessage(conn *net.TCPConn, topicName, partitionNum string, targetOffset, count uint32) {
	var request common.Request
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
		fmt.Printf("written length [%d] is too small than: [%d]\n", n, requestTotalLen)
		return
	} else {
		fmt.Printf("written %d bytes\n", n)
	}

	if err != nil {
		fmt.Println("netbuffer.WriteTo failed:", err)
		return
	}

	if n != int64(requestTotalLen) {
		fmt.Printf("written length [%d] is too small than: [%d]\n", n, requestTotalLen)
		return
	} else {
		fmt.Printf("written %d bytes\n", n)
	}

	for {
		offsetBuf := make([]byte, 4)
		lengthBuf := make([]byte, 4)

		readN, err := conn.Read(offsetBuf)
		if readN == 0 || err != nil {
			break
		}

		offset := binary.LittleEndian.Uint32(offsetBuf)

		readN, err = conn.Read(lengthBuf)
		if readN == 0 || err != nil {
			break
		}

		length := binary.LittleEndian.Uint32(lengthBuf)

		fmt.Printf("offset: %d, length: %d\n", offset, length)

		bodyBuf := make([]byte, length)

		readN, err = conn.Read(bodyBuf)
		if readN == 0 || err != nil {
			break
		}

		body := common.BytesToMessage(bodyBuf)
		fmt.Println("body: ", body)

		key := bodyBuf[common.WRITE_MESSAGE_LEN : common.WRITE_MESSAGE_LEN+body.KeyLength]
		value := bodyBuf[common.WRITE_MESSAGE_LEN+body.KeyLength : common.WRITE_MESSAGE_LEN+body.KeyLength+body.ValueLength]
		fmt.Println("key: ", string(key))
		fmt.Println("value: ", string(value))
	}
}

func main() {
	flag.Parse()

	addr := &net.TCPAddr{net.ParseIP(*serverAddress), *serverPort, ""}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		fmt.Println("dial failed:", err)
		return
	}

	if *action == "" {
		fmt.Println("please specify action!")
		os.Exit(1)
	}

	if *action == "write" {
		WriteMessage(conn)
	} else if *action == "read" {
		ReadMessage(conn, "mytopic", "0", 1, 5)
	} else if *action == "createTopic" {
		CreateTopic(conn, "mytopic", 3, 3)
	} else {
		fmt.Println("action is not support!")
		os.Exit(1)
	}
}
