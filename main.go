package main

import (
	"fmt"
	"github.com/shelmesky/gms/log"
	"os"
	"time"
)

func main() {
	filename := "./0000000000000000000.log"
	capacity := 1024*1024* 2
	var offset int = 0

	log := disklog.DiskLog{}
	log.Init("./data-dir")

	err := disklog.CreateLogSegment(filename, capacity)
	if err != nil {
		fmt.Println(err)
	}

	logSegmentRW, err := disklog.OpenReadWriteLogSegment(filename, capacity)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	logSegmentRDOnly, err := disklog.OpenRDOnlyLogSegment(filename, capacity)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	/*******************************************************************************/
	fmt.Println("")

	data := []byte("111111111")
	dataLength := len(data)
	logSegmentRW.AppendBytes(data, dataLength)
	fmt.Println("data written:", logSegmentRW.Used())

	err = logSegmentRW.Force()
	if err != nil {
		fmt.Println("sync memory data to disk failed:", err)
	}

	var result []byte
	result, err = logSegmentRDOnly.ReadBytes(0, dataLength)
	if err != nil {
		fmt.Println(err)
		os.Exit(-2)
	}
	fmt.Println("read bytes:", string(result))
	offset += dataLength

	/*******************************************************************************/
	fmt.Println("")

	data1 := []byte("222222222")
	dataLength1 := len(data1)
	logSegmentRW.AppendBytes(data1, dataLength1)
	fmt.Println("data written:", logSegmentRW.Used())

	var result1 []byte
	result1, err = logSegmentRDOnly.ReadBytes(offset, dataLength1)
	if err != nil {
		fmt.Println(err)
		os.Exit(-2)
	}
	fmt.Println("read bytes:", string(result1))
	offset += dataLength1

	/*******************************************************************************/
	fmt.Println("")

	logSegmentRW.AppendUInt32(1234567)
	fmt.Println("data written:", logSegmentRW.Used())

	var result2 uint32
	result2, err = logSegmentRDOnly.ReadUInt32(offset)
	fmt.Println("read uint32:", result2)
	offset += 4

	/*******************************************************************************/
	fmt.Println("")

	logSegmentRW.AppendUInt64(987654321)
	fmt.Println("data written:", logSegmentRW.Used())

	var result3 uint64
	result3, err = logSegmentRDOnly.ReadUInt64(offset)
	fmt.Println("read uint64:", result3)
	offset += 8

	time.Sleep(time.Second * 60)
	fmt.Println("program exit...")
}
