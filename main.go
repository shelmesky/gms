package main

import (
	"fmt"
	"github.com/shelmesky/gms/log"
	"os"
)

func main() {
	filename := "./00000001.log"
	capacity := 1024*1024*1

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
	logSegment.Close()

	fmt.Println("hello world")
}
