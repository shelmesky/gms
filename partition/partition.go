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
	dataDir = "./data"
)

// 单个partition
type Partition struct {
	dirName string          // 目录名
	log     disklog.DiskLog // 日志管理器
	queue   chan *common.Message
}

// 一组partition
type PartitionList struct {
	dirName       string             // 存储一组partition的顶层目录名
	topicName     string             // topic名
	numPartitions int                // partition的数量
	partitions    map[int]*Partition // 多个partition组成的map
}

// 在目录下创建n个partition
// dirName即是topic name
func CreatePartitionList(dirName string, numPartitions int) error {
	if err := os.Chdir(dataDir); err != nil {
		return  err
	}
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		err = os.Mkdir(dirName, 0775)
		if err != nil {
			return err
		}

		// 创建N个partition, 序号从0开始
		for i := 0; i < numPartitions; i++ {
			partitionDirName := path.Join(dirName, dirName+"-"+strconv.Itoa(i))
			err = os.Mkdir(partitionDirName, 0775)
			if err != nil {
				return err
			}

			var log disklog.DiskLog
			err = log.Init(partitionDirName)
			if err != nil {
				return err
			}
		}
	} else {
		return utils.FileAlreadyExist
	}
	return nil
}

// 读取dirName下的文件夹并初始化PartitionList
func (partitionList *PartitionList) Init(dirName string, numPartitions int) {
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
