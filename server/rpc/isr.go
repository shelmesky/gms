package rpc

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"sync"
)

var (
	GlobalFollowerManager *FollowerManager
)

func init() {
	GlobalFollowerManager = NewFollowerManager()
}

/*
1. 管理所有follower.
2. 所有follower之中和leader同样offset， 且1分钟之内有请求的节点保存在ISR中.
3. leader中保存的某个offset在当所有follower节点同步之后， 才会让consumer读取这个offset.
4. followerMap中KEY是TopicName + PartitionIndex + ReplicaIndex， VALUE是Follower对象
*/
type FollowerManager struct {
	topicPartitionReplicaMap map[string]*Follower
	sync.RWMutex
}

func NewFollowerManager() *FollowerManager {
	var followerManager FollowerManager
	followerManager.topicPartitionReplicaMap = make(map[string]*Follower)
	return &followerManager
}

func (this *FollowerManager) makeKey(topicName string, partitionIndex, replicaIndex int) string {
	if replicaIndex == 0 {
		return topicName + strconv.Itoa(partitionIndex)
	}
	return topicName + strconv.Itoa(partitionIndex) + strconv.Itoa(replicaIndex)
}

func (this *FollowerManager) Add(follower Follower) {
	log.Println("FollowerManager() Add follower:", follower)
	key := this.makeKey(follower.TopicName, follower.PartitionIndex, follower.Replica)

	this.Lock()
	defer this.Unlock()

	if _, ok := this.topicPartitionReplicaMap[key]; !ok {
		follower.WaitChan = make(chan int, 1024)
		follower.MessageChan = make(chan int, 1024)
		this.topicPartitionReplicaMap[key] = &follower
	}
}

func (this *FollowerManager) Get(follower Follower) *Follower {
	var ok bool
	var targetFollower *Follower

	this.RLock()
	defer this.RUnlock()

	key := this.makeKey(follower.TopicName, follower.PartitionIndex, follower.Replica)
	if targetFollower, ok = this.topicPartitionReplicaMap[key]; ok {
		return targetFollower
	}

	return nil
}

func (this *FollowerManager) PutOffset(follower Follower) error {
	targetFollower := this.Get(follower)
	if targetFollower != nil {
		targetFollower.WaitChan <- follower.Offset
		log.Println("##################### PufOffset:", follower)
	} else {
		return fmt.Errorf("FollowerManager cant find %v\n", follower)
	}

	return nil
}

func (this *FollowerManager) WaitOffset(topicName string, partitionIndex, currentOffset int) error {
	var followerList []*Follower

	key := this.makeKey(topicName, partitionIndex, 0)

	this.RLock()
	defer this.RUnlock()

	// leader当前的currentOffset大于follow
	currentOffset -= 1

	for k, v := range this.topicPartitionReplicaMap {
		if strings.HasPrefix(k, key) {
			// 向MessageChan中放入最新offset， 告诉follower有新消息可以读取
			v.MessageChan <- currentOffset

			followerList = append(followerList, v)
		}
	}

	listLen := len(followerList)

	if listLen == 0 {
		return fmt.Errorf("WaitOffset() cant find any follower!")
	}

	for idx := range followerList {
		targetFollower := followerList[idx]
		// 在循环中多次读取WaitChan， 因为follower可能在其中放入多个offset
		// 当follower的offset落后leader时会发生这种情况
		for {
			followerOffset := <-targetFollower.WaitChan
			if followerOffset == currentOffset {
				log.Println("##################### WaitOffset:", followerOffset, currentOffset)
				listLen -= 1
				break
			}
		}
	}

	if listLen == 0 {
		log.Debugln("##################### WaitOffset:", listLen)
		return nil
	}

	return fmt.Errorf("FollowerManager WaitOffset failed: [%s - %d - %d]\n",
		topicName, partitionIndex, currentOffset)
}

func (this FollowerManager) String() {
	for k, v := range this.topicPartitionReplicaMap {
		log.Debugf("[%s] -> [%v]\n", k, v)
	}
}
