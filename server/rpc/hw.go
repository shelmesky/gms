package rpc

import (
	"encoding/json"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/topics"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type OffsetEntry struct {
	TopicName    string
	PartitionIdx int
	ReplicaIdx   int
	Offset       int
	ReplicaNum   int
}

type HWItem struct {
	receivedNum int
	allReceived bool
}

/*
	节点作为副本的Leader管理从rpc follower发来的offset信息，
	并启动goroutine将这些信息收集然后判断一个offset是为被所有follower接收.
*/
type HWManager struct {
	HWChan         chan OffsetEntry
	HWCollectMap   map[string]*HWItem
	CollectMapLock sync.RWMutex
}

var (
	PartitionHWManager *HWManager
)

func init() {
	PartitionHWManager = new(HWManager)
	PartitionHWManager.HWChan = make(chan OffsetEntry, 1024)
	PartitionHWManager.HWCollectMap = make(map[string]*HWItem, 1024)
	go PartitionHWManager.Run() // 进程启动时运行
}

func (this *HWManager) makeOffsetEntryKey(entry OffsetEntry) string {
	var strList []string

	topicName := entry.TopicName
	partitionIdx := strconv.Itoa(entry.PartitionIdx)
	// 因为HW代表的是除了leader以外的其他所有follower是否全部收到某offset
	// 所以这个Add时的key不能包括ReplicaIdx)
	//replicaIdx := strconv.Itoa(entry.ReplicaIdx)
	offset := strconv.Itoa(entry.Offset)

	strList = append(strList, topicName)
	strList = append(strList, partitionIdx)
	//strList = append(strList, replicaIdx)
	strList = append(strList, offset)

	return strings.Join(strList, ":")
}

func (this *HWManager) makePartitionHWKey(entry OffsetEntry) string {
	var strList []string

	topicName := entry.TopicName
	partitionIdx := strconv.Itoa(entry.PartitionIdx)

	strList = append(strList, topicName)
	strList = append(strList, partitionIdx)

	return strings.Join(strList, ":")
}

func (this *HWManager) Add(entry OffsetEntry) error {
	select {
	case <-common.GlobalTimingWheel.After(50 * time.Millisecond):
		return fmt.Errorf("HWManager.Add() timeout...")
	case this.HWChan <- entry:
		return nil
	}
}

func (this *HWManager) Run() {
	this.CollectMapLock.Lock()
	defer this.CollectMapLock.Unlock()

	log.Println("HWManager.Run() ...")

	for {
		entry := <-this.HWChan
		log.Println("0000000000000000000000000", entry)
		key := this.makeOffsetEntryKey(entry)

		if tempHWItem, ok := this.HWCollectMap[key]; !ok {
			var item HWItem
			item.allReceived = false
			item.receivedNum = 1 // 第一次创建就应该+1
			this.HWCollectMap[key] = &item
		} else {
			// 自增已经收到的副本数
			tempHWItem.receivedNum += 1

			// 如果map中存在了key
			// 首先判断已经收到的副本数量是否已经满足(follower副本离线)
			// 满足了直接设置已经收到为true
			if tempHWItem.receivedNum == entry.ReplicaNum {
				tempHWItem.allReceived = true
			}

			// 如果设置了副本被所有follower接收
			// 则将offset信息保存在map[topicName+partitionIndex]offset中
			if tempHWItem.allReceived == true {
				topic := topics.TopicManager.GetTopic(entry.TopicName)
				if topic != nil {
					partition := topic.GetPartition(entry.PartitionIdx)
					if partition != nil {
						atomic.StoreUint64(&partition.HW, uint64(entry.Offset))

						// 作为副本的leader保存当前自己记录的HW值到etcd
						// 设置replicaIndex为0, 强制指定为leader副本
						//go SaveHWToEtcd(entry.TopicName, entry.PartitionIdx, 0, entry.Offset)
						err := SaveHWToEtcd(entry.TopicName, entry.PartitionIdx, 0, entry.Offset)
						log.Println("1111111111111111111111111", entry)
						if err != nil {
							log.Errorln("HWManager.Run() SaveHWToEtcd failed:", err)
						}
					}
				}
			}
		}
	}
}

// 根据分区的index获取保存在Partition结构中的字段HW
func (this *HWManager) GetPartitionHW(topicName string, partitionIndex int) uint64 {
	topicManager := topics.TopicManager

	topic := topicManager.GetTopic(topicName)
	if topic != nil {
		partition := topic.GetPartition(partitionIndex)
		if partition != nil {
			return atomic.LoadUint64(&partition.HW)
		}
	}
	return 0
}

// 每2秒保存分区的HW到etcd
func SaveHWToEtcd(topicName string, partitionIndex, replicaIndex, HW int) error {
	now := time.Now().UnixNano()
	if now%2 == 0 {
		key := fmt.Sprintf("/topics-brokers/%s/partition-%d/replica-%d",
			topicName, partitionIndex, replicaIndex)

		// 在etcd获取某个分区副本的详细信息
		getResp, err := common.ETCDGetKey(key, false)
		if err != nil {
			return err
		}

		if len(getResp.Kvs) == 0 {
			return nil
		}

		valueBytes := getResp.Kvs[0].Value

		var nodeParRep NodePartitionReplicaInfo
		err = json.Unmarshal(valueBytes, &nodeParRep)
		if err != nil {
			return err
		}

		// 赋值HW
		nodeParRep.HW = HW

		value, err := json.Marshal(nodeParRep)
		if err != nil {
			return err
		}

		// 在etcd上保存更新HW之后的分区副本信息s
		_, err = common.ETCDPutKey(key, string(value))

		if err != nil {
			return err
		}
	}

	return nil
}
