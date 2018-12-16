package main

import (
	"encoding/binary"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"io"
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
func NewBody(key, value []byte) ([]byte, uint32) {
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

	return messageBytes, uint32(message.Length)
}

func NewWriteMessageMeta(topicName, partitionNum string) ([]byte, uint32) {
	meta := common.NewWriteMessageAction(topicName, partitionNum)
	return meta, uint32(len(meta))
}

func WriteMessage(topicName, PartitionNum string, bodyKey, bodyValue []byte, conn *net.TCPConn) {
	var netBuffer net.Buffers

	var request common.Request
	var MetaData []byte
	var messageHead []byte

	request.Version = 1001
	request.Sequence = 1

	MetaData, request.MetaDataLength = NewWriteMessageMeta(topicName, PartitionNum)

	messageHead, request.BodyLength = NewBody(bodyKey, bodyValue)

	requestLength := common.REQUEST_LEN
	request.TotalLength = uint64(requestLength + int(request.MetaDataLength) + int(request.BodyLength))

	requestBytes := common.RequestToBytes(&request)

	// total length of whole message
	totalLenBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(totalLenBuf, request.TotalLength)

	netBuffer = append(netBuffer, totalLenBuf)
	netBuffer = append(netBuffer, requestBytes)
	netBuffer = append(netBuffer, MetaData)
	netBuffer = append(netBuffer, messageHead)
	netBuffer = append(netBuffer, bodyKey)
	netBuffer = append(netBuffer, bodyValue)
	n, err := netBuffer.WriteTo(conn) // this will use writev in linux
	if err == io.EOF {
		fmt.Println("connection lost")
		return
	}

	if n-8 != int64(request.TotalLength) {
		fmt.Printf("write not satisfied [%d] length: %d\n", request.TotalLength, n)
		return
	}

	fmt.Printf("write [%d] bytes\n", n)
}

func main() {
	addr := &net.TCPAddr{net.ParseIP("127.0.0."), 50051, ""}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		fmt.Println("dial failed:", err)
		return
	}

	key := []byte("key")
	value := []byte("value")
	WriteMessage("mytopic", "12", key, value, conn)
}
