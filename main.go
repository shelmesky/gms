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
		os.Exit(1)
	}

	data := []byte("abcdefghijklmn")
	dataLength := len(data)
	logSegment.AppendBytes(data, dataLength)
	fmt.Println("data written:", logSegment.Used())
	err = logSegment.Force()
	if err != nil {
		fmt.Println("sync memory data to disk failed:", err)
	}

	time.Sleep(time.Second * 60)

	fmt.Println("hello world")
}
