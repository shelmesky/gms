package partition

import (
	"github.com/shelmesky/gms/common"
	"github.com/shelmesky/gms/log"
	"github.com/shelmesky/gms/utils"
	"os"
	"path"
	"strconv"
)

const (
	dataDir          = "./data"
	maxPartitionNums = 99
)

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
func CreatePartitionList(dirName string, numPartitions int) error {
	var err error

	if _, err = os.Stat(dirName); os.IsNotExist(err) {
		err = os.Mkdir(dirName, 0775)
		if err != nil {
			goto failed
		}

		// 创建N个partition, 序号从0开始
		for i := 0; i < numPartitions; i++ {
			partitionDirName := path.Join(dirName, dirName+"-"+strconv.Itoa(i))
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
func (partitionList *PartitionList) AppendMessage(topic string, partition int, message *common.Message) {

}

// 发送消息到socket fd
func (patitionList *PartitionList) SendDataToSock() {

}

func (PartitionList *PartitionList) StartProcessor() {

}
