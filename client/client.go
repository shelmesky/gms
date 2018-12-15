package main

import (
	"encoding/binary"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"net"
	"time"
)

const (
	address = "127.0.0.1:50051"
)

func main() {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("dial failed:", err)
		return
	}

	var request common.Request
	request.Version = 1001
	request.Sequence = 1
	request.MetaDataLength = 6
	request.BodyLength = 6

	MetaData := []byte("abcdef")
	Body := []byte("123456")


	requestLength := common.REQUEST_LEN

	MetaDataLen := len(MetaData)
	BodyLen := len(Body)

	length := requestLength + MetaDataLen + BodyLen

	request.TotalLength = uint64(length)
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


	// send meta data
	n, err = conn.Write(MetaData)
	if err != nil {
		fmt.Println("write failed:", err)
		return
	}
	if n != MetaDataLen {
		fmt.Println("write not satisfied length:", n)
		return
	}

	// send body
	n, err = conn.Write(Body)
	if err != nil {
		fmt.Println("write failed:", err)
		return
	}
	if n != BodyLen {
		fmt.Println("write not satisfied length:", n)
		return
	}

	time.Sleep(60 * time.Second)
}
