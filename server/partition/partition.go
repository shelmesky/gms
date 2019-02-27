package partition

import (
	"encoding/hex"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/log"
	"github.com/shelmesky/gms/server/utils"
	"github.com/shelmesky/highwayhash"
	"math/rand"
	"os"
	"path"
	"strconv"
)

const (
	maxPartitionNums = 99 // 初始化时最多读取此数量的分区
	KeyString        = "010102030405060807090A0B0C0D0E0FF0E0D0C0B0A090807060504030901000"
)

func Hash(data []byte) uint64 {
	key, _ := hex.DecodeString(KeyString)
	return highwayhash.Sum64(data, key)
}

// 单个partition
// 将为每个partition启动一个线程
type Partition struct {
	dirName string           // 目录名
	log     *disklog.DiskLog // 日志管理器
	queue   chan []byte
}

func (p *Partition) GetLog() *disklog.DiskLog {
	return p.log
}

// 一组partition
// 拥有同样的topicName
type PartitionList struct {
	topicName     string             // topic名
	numPartitions int                // partition的数量
	partitions    map[int]*Partition // 多个partition组成的map
}

// 在目录下创建n个partition
// dirName即是topic name
func CreatePartitionList(topicName string, numPartitions int) error {
	var err error

	if _, err = os.Stat(topicName); os.IsNotExist(err) {
		err = os.Mkdir(topicName, 0775)
		if err != nil {
			goto failed
		}

		// 创建N个partition, 序号从0开始
		for i := 0; i < numPartitions; i++ {
			partitionDirName := path.Join(topicName, topicName+"-"+strconv.Itoa(i))
			err = os.Mkdir(partitionDirName, 0775)
			if err != nil {
				goto failed
			}

			var log disklog.DiskLog
			err = log.Init(partitionDirName)
			if err != nil {
				goto failed
			}
		}

		goto success
	} else {
		err = utils.FileAlreadyExist
		goto failed
	}

failed:
	return err
success:
	return nil
}

func (partitionList *PartitionList) GetPartition(partitionIndex int) *Partition {
	if value, ok := partitionList.partitions[partitionIndex]; ok {
		return value
	}

	return nil
}

// 读取dirName下的文件夹并初始化PartitionList
func (partitionList *PartitionList) Init(topicName string) error {

	partitionList.partitions = make(map[int]*Partition, 8)

	// 循环读取maxPartitionNums个目录
	for i := 0; i < maxPartitionNums; i++ {
		partitionDirName := path.Join(topicName, topicName+"-"+strconv.Itoa(i))
		if _, err := os.Stat(partitionDirName); !os.IsNotExist(err) {
			var partition Partition
			var log disklog.DiskLog

			partition.dirName = partitionDirName

			err = log.Init(partitionDirName)
			if err != nil {
				return err
			}

			partition.log = &log
			partition.queue = make(chan []byte, 1024)
			partitionList.partitions[i] = &partition
		}
	}

	partitionList.topicName = topicName
	partitionList.numPartitions = len(partitionList.partitions)

	return nil
}

/*
追加消息到topic
partitionIndex: 分区对应的序号
body: 消息体本身
bodyLen: 消息体长度
*/
func (partitionList *PartitionList) AppendMessage(partitionIndex string, body []byte, bodyLen int) error {
	var selectedPartition int
	var err error

	// 获取第一个消息的头部
	firstMessageHeader := common.BytesToMessage(body[:common.WRITE_MESSAGE_LEN])

	// 如果只有一个消息
	if firstMessageHeader.Length == uint64(bodyLen) {

		KeyPayload := body[common.WRITE_MESSAGE_LEN : common.WRITE_MESSAGE_LEN+firstMessageHeader.KeyLength]
		ValuePayload := body[common.WRITE_MESSAGE_LEN+firstMessageHeader.KeyLength : common.WRITE_MESSAGE_LEN+
			firstMessageHeader.KeyLength+firstMessageHeader.ValueLength]

		fmt.Println("======================================================")
		fmt.Printf("body length: %d\n", firstMessageHeader)
		fmt.Printf("firstMessage: %d %v\n", common.WRITE_MESSAGE_LEN, firstMessageHeader)
		fmt.Printf("message key: %d %s, %v\n", len(KeyPayload), string(KeyPayload), KeyPayload)
		fmt.Printf("message value: %d %s, %v\n", len(ValuePayload), string(ValuePayload), ValuePayload)

		if len(partitionIndex) > 0 {
			selectedPartition, err = strconv.Atoi(partitionIndex)
			if err != nil {
				return err
			}
		} else {
			if firstMessageHeader.KeyLength > 0 {
				KeyPayload := body[common.WRITE_MESSAGE_LEN : common.WRITE_MESSAGE_LEN+firstMessageHeader.KeyLength]
				keyHash := uint64(Hash(KeyPayload))
				selectedPartition = int(keyHash % uint64(partitionList.numPartitions))
			} else {
				selectedPartition = rand.Int() % partitionList.numPartitions
			}
		}

		if partition, ok := partitionList.partitions[selectedPartition]; ok {
			if partition != nil {
				partition.queue <- body
			}
		} else {
			return utils.PartitionNotExist
		}

	} else if firstMessageHeader.Length < uint64(bodyLen) { // 多个消息
		pos := uint64(0)
		length := uint64(bodyLen)
		for {
			// 如果已经读了所有消息
			if length == 0 {
				break
			}

			// 如果剩余的字节数不足一个消息头部
			if length < common.WRITE_MESSAGE_LEN {
				return utils.MessageLengthInvalid
			}

			fmt.Println("length: ", length)

			messageHeader := common.BytesToMessage(body[pos : pos+common.WRITE_MESSAGE_LEN])

			KeyPayload := body[pos+common.WRITE_MESSAGE_LEN : pos+common.WRITE_MESSAGE_LEN+messageHeader.KeyLength]
			ValuePayload := body[pos+common.WRITE_MESSAGE_LEN+messageHeader.KeyLength : pos+common.WRITE_MESSAGE_LEN+
				messageHeader.KeyLength+messageHeader.ValueLength]
			fmt.Println("======================================================")
			fmt.Printf("body length: %d\n", messageHeader.Length)
			fmt.Printf("messageHeader:%d %v\n", common.WRITE_MESSAGE_LEN, messageHeader)
			fmt.Printf("message key: %d %s, %v\n", len(KeyPayload), string(KeyPayload), KeyPayload)
			fmt.Printf("message value: %d %s, %v\n", len(ValuePayload), string(ValuePayload), ValuePayload)

			// 如果剩余的字节数不足一个完整的消息
			if length < messageHeader.Length {
				return utils.MessageLengthInvalid
			}

			messageBytes := body[pos : pos+messageHeader.Length]

			if len(partitionIndex) > 0 {
				selectedPartition, err = strconv.Atoi(partitionIndex)
				if err != nil {
					return err
				}
			} else {
				if messageHeader.KeyLength > 0 {
					KeyPayload := messageBytes[common.WRITE_MESSAGE_LEN : common.WRITE_MESSAGE_LEN+messageHeader.KeyLength]
					keyHash := uint64(Hash(KeyPayload))
					selectedPartition = int(keyHash % uint64(partitionList.numPartitions))
				} else {
					selectedPartition = rand.Int() % partitionList.numPartitions
				}
			}

			if partition, ok := partitionList.partitions[selectedPartition]; ok {
				if partition != nil {
					partition.queue <- body[pos : pos+messageHeader.Length]
				}
			} else {
				return utils.PartitionNotExist
			}

			length -= messageHeader.Length
			pos += messageHeader.Length
		}
	}

	return nil
}

// 发送消息到socket fd
func (patitionList *PartitionList) SendDataToSock() {

}

func (patitionList *PartitionList) StartWorker() {
	for idx, value := range patitionList.partitions {
		go func(idx int, partition *Partition) {
			fmt.Printf("goroutine start for partition: [%s].\n", partition.dirName)
			for {
				messageBytes := <-partition.queue
				dataWritten, err := partition.log.AppendBytes(messageBytes, len(messageBytes))
				if err != nil {
					fmt.Println("append bytes to log failed:", err)
				}
				fmt.Printf("write [%d] bytes message to partition: [%s].\n", dataWritten, partition.dirName)
			}
		}(idx, value)
	}
}
