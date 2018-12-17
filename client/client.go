package main

import (
	"encoding/binary"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"net"
)

const (
	address = "127.0.0.1:50051"
)

func CRC32(data []byte) {

}

/*
传入key和value创建消息
返回消息的bytes和消息的总长度(head+key+value)
*/
func NewBody(key, value []byte) ([]byte, uint64) {
	var message common.MessageType
	message.CRC32 = 0
	message.Magic = 0
	message.Attributes = 0
	message.KeyLength = uint64(len(key))
	message.ValueLength = uint64(len(value))

	messageBytesLen := common.MESSAGE_LEN

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

func main() {
	addr := &net.TCPAddr{net.ParseIP("127.0.0."), 50051, ""}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		fmt.Println("dial failed:", err)
		return
	}

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
	for i := 0; i < 1; i++ {
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
	if n != int64(requestTotalLen) {
		fmt.Printf("written length [%d] is too small than: [%d]\n", n, requestTotalLen)
		return
	} else {
		fmt.Printf("written %d bytes\n", n)
	}

	// 发送N个消息
	n, err = netBufferBody.WriteTo(conn)
	if uint32(n) != requestTotalLen {
		fmt.Printf("written length [%d] is too small than: [%d]\n", n, totalBodyLength)
		return
	} else {
		fmt.Printf("written %d bytes\n", n)
	}
}
