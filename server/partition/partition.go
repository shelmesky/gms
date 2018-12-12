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
	maxPartitionNums = 99
	KeyString        = "010102030405060807090A0B0C0D0E0FF0E0D0C0B0A090807060504030901000"
)

func Hash(data []byte) uint64 {
	key, _ := hex.DecodeString(KeyString)
	return highwayhash.Sum64([]byte("aa1"), key)
}

// 单个partition
// 将为每个partition启动一个线程
type Partition struct {
	dirName string          // 目录名
	log     disklog.DiskLog // 日志管理器
	queue   chan *common.Message
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

			partition.log = log
			partition.queue = make(chan *common.Message, 1024)
			partitionList.partitions[i] = &partition
		}
	}

	partitionList.topicName = topicName
	partitionList.numPartitions = len(partitionList.partitions)

	return nil
}

// 追加消息到topic
// topic: 标题名称
// partition: 分区序号
// message: 消息内容
func (partitionList *PartitionList) AppendMessage(partition int, message *common.Message) {
	var selectedPartition int

	if message.KeyLength > 0 {
		selectedPartition = int(Hash(message.KeyPayload) % uint64(partitionList.numPartitions))
	} else {
		selectedPartition = rand.Int() % partitionList.numPartitions
	}

	if partition, ok := partitionList.partitions[selectedPartition]; ok {
		if partition != nil {
			partition.queue <- message
		}
	}
}

// 发送消息到socket fd
func (patitionList *PartitionList) SendDataToSock() {

}

func (patitionList *PartitionList) StartProcessor() {
	for idx, partition := range patitionList.partitions {
		go func(idx int) {
			for {
				message := <-partition.queue
				messageBytes := message.Bytes()
				dataWritten, err := partition.log.AppendBytes(messageBytes, len(messageBytes))
				if err != nil {
					fmt.Println("append bytes to log failed:", err)
				}
				fmt.Printf("write [%d] bytes message.\n", dataWritten)
			}
		}(idx)
	}
}
