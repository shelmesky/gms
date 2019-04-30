package partition

import (
	"encoding/hex"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/log"
	"github.com/shelmesky/gms/server/utils"
	"github.com/shelmesky/highwayhash"
	log "github.com/sirupsen/logrus"
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

/*
	单个partition
 	将为每个partition启动一个goroutine

	ReplicasNum字段：
	ReplicasNum在刚启动时是在RPCHandle_SYNC_MANAGER处理命令时获取第一次，
	之后当每次监控到发生了某个节点离线，就会找到这个节点上所有的分区副本，
	并将leader本地的Partition结构体的ReplicasNum减1.

	HW字段:
	如果是Follower， 每次接收消息都将消息体中来自Leader的HW属性值保存。
	Follower启动一个goroutine， 负责将所有topic的所有分区的HW值定时保存在/topics-brokers/对应的KEY下面。

	如果是Leader， 由HWManager起动一个goroutine来管理分区的HW并在消息同步中发送给Follower.
	并在HWManager的goroutine之中， 定时将HW值保存在/topics-brokers/对应的KEY下面。
*/
type Partition struct {
	dirName     string           // 目录名
	diskLog     *disklog.DiskLog // 日志管理器
	queue       chan []byte      // 每个Partition启动一个goroutine处理消息写入
	HW          uint64           // 保存了当前节点上分区的HW
	ReplicasNum int              // 作为leader副本保存了当前有几个follower副本
}

func (p *Partition) GetLog() *disklog.DiskLog {
	return p.diskLog
}

func (p *Partition) GetCurrentOffset() int {
	activeSegment := p.diskLog.GetActiveSegment()
	return activeSegment.GetCurrentOffset()
}

// 在目录下创建n个partition
// dirName即是topic name
func CreatePartitionList(topicName string, numPartitions int) error {
	for i := 0; i < numPartitions; i++ {
		err := CreatePartition(topicName, i)
		if err != nil {
			return err
		}
	}

	return nil
}

// 根据topicName和partitionIndex创建目录并初始化内容
func CreatePartition(topicName string, partitionIndex int) error {
	var err error
	var partitionDirName string

	if _, err = os.Stat(topicName); os.IsNotExist(err) {
		err = os.Mkdir(topicName, 0775)
		if err != nil {
			goto failed
		}
	}

	partitionDirName = path.Join(topicName, topicName+"-"+strconv.Itoa(partitionIndex))

	if _, err = os.Stat(partitionDirName); os.IsNotExist(err) {
		// 根据序号创建partition
		err = os.Mkdir(partitionDirName, 0775)
		if err != nil {
			goto failed
		}

		var diskLog disklog.DiskLog
		err = diskLog.Init(partitionDirName)
		if err != nil {
			goto failed
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

// 一组partition
// 拥有同样的topicName
type PartitionList struct {
	topicName     string             // topic名
	numPartitions int                // partition的数量
	partitions    map[int]*Partition // 多个partition组成的map
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
			var diskLog disklog.DiskLog

			partition.dirName = partitionDirName

			// log文件初始化
			err = diskLog.Init(partitionDirName)
			if err != nil {
				return err
			}

			partition.diskLog = &diskLog
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

		// 如果参数提供了想要写入的分区编号
		if len(partitionIndex) > 0 {
			selectedPartition, err = strconv.Atoi(partitionIndex)
			if err != nil {
				return err
			}
		} else {
			// 如果参数未提供分区编号，则尝试根据key来hash，然后找到分区编号
			if firstMessageHeader.KeyLength > 0 {
				KeyPayload := body[common.WRITE_MESSAGE_LEN : common.WRITE_MESSAGE_LEN+firstMessageHeader.KeyLength]
				keyHash := uint64(Hash(KeyPayload))
				selectedPartition = int(keyHash % uint64(partitionList.numPartitions))
			} else {
				// 如果key也没有提供， 则在多个分区中随机
				selectedPartition = rand.Int() % partitionList.numPartitions
			}
		}

		// 如果在打开的topic列表中根据分区编号找到分区， 则将body写入到分区的worker chan
		if partition, ok := partitionList.partitions[selectedPartition]; ok {
			if partition != nil {
				//partition.queue <- body
				partition.diskLog.AppendBytes(body, len(body))
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
					//partition.queue <- body[pos : pos+messageHeader.Length]
					message := body[pos : pos+messageHeader.Length]
					partition.diskLog.AppendBytes(message, len(message))
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

// 为一个topic下的所有分区列表启动worker chan.
// 当在磁盘上找到一个分区，即路径为topicxxx/topicxxx-0， 则会启动一个goroutine接收数据并写入到这个分区.
func (patitionList *PartitionList) StartWorker() {
	for idx, value := range patitionList.partitions {
		go func(idx int, partition *Partition) {
			log.Printf("goroutine start for partition: [%s].\n", partition.dirName)
			for {
				messageBytes := <-partition.queue
				dataWritten, err := partition.diskLog.AppendBytes(messageBytes, len(messageBytes))
				if err != nil {
					log.Errorln("append bytes to log failed:", err)
				}
				log.Printf("write [%d] bytes message to partition: [%s].\n", dataWritten, partition.dirName)
			}
		}(idx, value)
	}
}
