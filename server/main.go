package main

import (
	"fmt"
	"github.com/shelmesky/gms/server/log"
	"github.com/shelmesky/gms/server/partition"
	"github.com/shelmesky/gms/server/server"
	"os"
	"time"
)

const (
	address = "0.0.0.0"
	port    = 50051
)

/*
func SendMessage(ctx context.Context, in *pb.WriteMessageRequest) (*pb.WriteMessageResponse, error) {
	//fmt.Println("receive request:", *in)
	topicName := in.TopicName

	if len(topicName) > 0 {
		topic := topicManager.GetTopic(topicName)

		if topic != nil {
			partitionIndex := in.Partition
			err := topic.AppendMessage(partitionIndex, in)
			if err != nil {
				return &pb.WriteMessageResponse{Code: -1, Result: err.Error()}, err
			}
			return &pb.WriteMessageResponse{Code: 0}, nil

		} else {
			return &pb.WriteMessageResponse{Code: -2, Result: utils.ServerError.Error()},
				utils.ServerError
		}

	}

	return &pb.WriteMessageResponse{Code: -3, Result: utils.ParameterTopicMissed.Error()},
		utils.ParameterTopicMissed
}
*/

func main() {
	//test1()
	server.Run(address, port)
}

func test1() {

	if err := os.Chdir("./data"); err != nil {
		fmt.Println("can not change to directory './data'")
		return
	}

	fmt.Println(partition.CreatePartitionList("mytopic", 3))

	var partitionList partition.PartitionList
	err := partitionList.Init("mytopic")
	if err != nil {
		fmt.Println(err)
	}

	/*
		var log disklog.DiskLog
		err := log.Init("./data")
		if err != nil {
			fmt.Println(err)
		}
	*/

	/*
		for i := 0; i < 20; i += 1 {
			str := strings.Repeat(strconv.Itoa(i), 6)
			data := fmt.Sprintf("{'%s': '%s'}", str, str)
			data_len := len(data)
			//fmt.Println(data, data_len)

			written, err := log.AppendBytes([]byte(data), data_len)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("[%d] data written: %d\n", i, written)
			}

		}
	*/

	/*
		batchRead := func(target int, length int) ([]byte, error) {
			var buffer []byte

			segmentPos, segment, logFilePos, err := log.Search(target)
			if err != nil {
				return buffer, err
			}

			if logFilePos < 0 {
				return buffer, fmt.Errorf("cant find offset\n")
			}

			fmt.Println(logFilePos, err)

			messageLength, err := segment.Log.ReadUInt32(logFilePos + 4)
			if err != nil {
				return buffer, err
			}

			messageContent, err := segment.Log.ReadBytes(logFilePos+8, int(messageLength))
			if err != nil {
				return buffer, nil
			}

			buffer = append(buffer, messageContent...)

			logFilePos += 8
			logFilePos += int(messageLength)

			// 减去上面读取的一条记录
			length -= 1

			readCounter := 0
			if segmentPos >= 0 && segmentPos <= log.SegmentLength()-1 {
				for {
					if readCounter >= length {
						break
					}

					// 消息的长度
					messageLength, err := segment.Log.ReadUInt32(logFilePos + 4)
					if err != nil {
						return buffer, err
					}

					// 说明读到了log文件末尾
					// 切换下一个segment并从文件开始处读
					if messageLength == 0 {
						segmentPos += 1
						segment, err = log.GetSegment(segmentPos)
						// 如果返回错误,说明已经读取完所有的segment
						if err != nil {
							return buffer, err
						}
						logFilePos = 0
						continue
					}

					messageContent, err := segment.Log.ReadBytes(logFilePos+8, int(messageLength))
					if err != nil {
						return buffer, nil
					}

					buffer = append(buffer, messageContent...)

					logFilePos += 8
					logFilePos += int(messageLength)
					readCounter += 1
				}
			}

			return buffer, nil
		}

		// TODO: target为0时, 二分搜索的lo >= hi都为0, 导致退出的BUG
		message, err := batchRead(35, 6)
		if err != nil && err != utils.IndexIsIllegal {
			fmt.Println(err)
		} else {
			fmt.Println(string(message))
		}
	*/

	/*
		filename := "./0000011111"
		logCapacity := 1024 * 1024 * 2
		indexCapacity := 1024 * 1024 * 1

		fmt.Println(disklog.CreateLogIndexSegmentFile(filename, logCapacity, indexCapacity))
		var logIndexSegment disklog.LogIndexSegment
		logIndexSegment.Open(filename, true, logCapacity, indexCapacity)
		err := logIndexSegment.LoadIndex()
		if err != nil && err != utils.EmptyIndexFile {
			fmt.Println(err)
			os.Exit(1)
		}

		logPos := logIndexSegment.Search(11119)
		fmt.Println(logIndexSegment.Log.ReadUInt32(logPos))
		logLength, err := logIndexSegment.Log.ReadUInt32(logPos + 4)
		fmt.Println(logLength, err)

		logContent, err := logIndexSegment.Log.ReadBytes(logPos+8, int(logLength))
		fmt.Println(string(logContent), err)
	*/

	/*
		data := "{'aaa': '111111'}"
		data_len := len(data)
		fmt.Println(logIndexSegment.AppendBytes([]byte(data), data_len))

		data = "{'bbb': '222222'}"
		data_len = len(data)
		fmt.Println(logIndexSegment.AppendBytes([]byte(data), data_len))
	*/
}

func test_segment() {

	filename := "./0000000000000000000.log"
	capacity := 1024 * 1024 * 2
	var offset int = 0

	log := disklog.DiskLog{}
	log.Init("./data-dir")

	err := disklog.CreateFile(filename, capacity)
	if err != nil {
		fmt.Println(err)
	}

	logSegmentRW, err := disklog.OpenReadWriteLogSegment(filename, capacity, true)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	logSegmentRDOnly, err := disklog.OpenRDOnlyLogSegment(filename, capacity, true)
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
