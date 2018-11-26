package main

import (
	"fmt"
	"github.com/shelmesky/gms/log"
	"os"
	"time"
)

func main() {
	filename := "./00000001.log"
	capacity := 1024*1024* 1024

	log := disklog.DiskLog{}
	log.Init("./data-dir")

	err := disklog.CreateLogSegment(filename, capacity)
	if err != nil {
		fmt.Println(err)
	}

	logSegment, err := disklog.OpenReadWriteLogSegment(filename, capacity)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	data := []byte("111111111")
	dataLength := len(data)
	logSegment.AppendBytes(data, dataLength)
	fmt.Println("data written:", logSegment.Used())

	err = logSegment.Force()
	if err != nil {
		fmt.Println("sync memory data to disk failed:", err)
	}

	var result []byte
	result, err = logSegment.ReadBytes(0, dataLength)
	if err != nil {
		fmt.Println(err)
		os.Exit(-2)
	}
	fmt.Println("read bytes:", string(result))


	data1 := []byte("222222222")
	dataLength1 := len(data1)
	logSegment.AppendBytes(data1, dataLength1)
	fmt.Println("data written:", logSegment.Used())

	var result1 []byte
	result1, err = logSegment.ReadBytes(dataLength, dataLength1)
	if err != nil {
		fmt.Println(err)
		os.Exit(-2)
	}
	fmt.Println("read bytes:", string(result1))


	time.Sleep(time.Second * 60)
	fmt.Println("program exit...")
}
