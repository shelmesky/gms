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

	messageBuffer := make([]byte, messageTotalLength)
	copy(messageBuffer, messageBytes)
	copy(messageBuffer[messageBytesLen:], key)
	copy(messageBuffer[messageBytesLen+int(message.KeyLength):], value)
	CRC32(messageBuffer)

	return messageBuffer, uint32(len(messageBuffer))
}

func NewWriteMessageMeta(topicName, partitionNum string) ([]byte, uint32) {
	meta := common.NewWriteMessageAction(topicName, partitionNum)
	return meta, uint32(len(meta))
}

func WriteMessage(topicName, PartitionNum string, bodyKey, bodyValue []byte, conn *net.TCPConn) {
	var request common.Request
	var MetaData []byte
	var Body []byte

	request.Version = 1001
	request.Sequence = 1

	MetaData, request.MetaDataLength = NewWriteMessageMeta(topicName, PartitionNum)

	Body, request.BodyLength = NewBody(bodyKey, bodyValue)

	requestLength := common.REQUEST_LEN
	request.TotalLength = uint64(requestLength + int(request.MetaDataLength) + int(request.BodyLength))

	requestBytes := common.RequestToBytes(&request)

	// send total length of whole message
	totalLenBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(totalLenBuf, request.TotalLength)
	n, err := conn.Write(totalLenBuf)
	if err != nil {
		fmt.Println("write failed:", err)
		return
	}
	if n != 8 {
		fmt.Println("write not satisfied length:", n)
		return
	}

	// send request head
	n, err = conn.Write(requestBytes)
	if err != nil {
		fmt.Println("write failed:", err)
		return
	}
	if n != requestLength {
		fmt.Println("write not satisfied length:", n)
		return
	}
	fmt.Println("send request len:", n)

	// send meta data
	n, err = conn.Write(MetaData)
	if err != nil {
		fmt.Println("write failed:", err)
		return
	}
	if n != int(request.MetaDataLength) {
		fmt.Println("write not satisfied length:", n)
		return
	}
	fmt.Println("send meta data len:", request.MetaDataLength)

	// send body
	n, err = conn.Write(Body)
	if err != nil {
		fmt.Println("write failed:", err)
		return
	}
	if n != int(request.BodyLength) {
		fmt.Println("write not satisfied length:", n)
		return
	}
	fmt.Println("send body len:", request.BodyLength)

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
