package partition

import (
	"encoding/hex"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/log"
	"github.com/shelmesky/gms/server/utils"
	"github.com/shelmesky/highwayhash"
	"os"
	"path"
	"strconv"
)

const (
	maxPartitionNums = 99
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
	queue   chan *common.MessageType
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
			partition.queue = make(chan *common.MessageType, 1024)
			partitionList.partitions[i] = &partition
		}
	}

	partitionList.topicName = topicName
	partitionList.numPartitions = len(partitionList.partitions)

	return nil
}

/*
// 追加消息到topic
// topic: 标题名称
// partition: 分区序号
// message: 消息内容
func (partitionList *PartitionList) AppendMessage(partitionIndex string, request *common.MessageType) error {
	var selectedPartition int
	var err error
	messagesLength := len(request.Message)

	for i:=0; i<messagesLength; i++ {
		message := request.Message[i]

		if len(partitionIndex) > 0 {
			selectedPartition, err = strconv.Atoi(partitionIndex)
			if err != nil {
				return err
			}
		} else {
			if message.KeyLength > 0 {
				keyHash := uint64(Hash(message.KeyPayload))
				selectedPartition = int(keyHash % uint64(partitionList.numPartitions))
			} else {
				selectedPartition = rand.Int() % partitionList.numPartitions
			}
		}

		if partition, ok := partitionList.partitions[selectedPartition]; ok {
			if partition != nil {
				partition.queue <- message
			}
		}
	}

	return nil
}
*/

// 发送消息到socket fd
func (patitionList *PartitionList) SendDataToSock() {

}

func (patitionList *PartitionList) StartWorker() {
	for idx, value := range patitionList.partitions {
		go func(idx int, partition *Partition) {
			fmt.Printf("goroutine start for partition: [%s].\n", partition.dirName)
			for {
				message := <-partition.queue
				messageBytes := common.MessageToBytes(message)
				dataWritten, err := partition.log.AppendBytes(messageBytes, len(messageBytes))
				if err != nil {
					fmt.Println("append bytes to log failed:", err)
				}
				fmt.Printf("write [%d] bytes message to partition: [%s].\n", dataWritten, partition.dirName)
			}
		}(idx, value)
	}
}
