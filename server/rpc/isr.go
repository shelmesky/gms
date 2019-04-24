package rpc

import (
	"fmt"
	"github.com/shelmesky/gms/server/common"
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

	for k, v := range this.topicPartitionReplicaMap {
		log.Println("111111111111111111111", k, key)
		if strings.HasPrefix(k, key) {

			log.Println("22222222222222222", k, key)

			v.MessageChan <- currentOffset

			// 排除自身节点
			if v.NodeID != common.GlobalConfig.NodeID {
				followerList = append(followerList, v)
			}
		}
	}

	listLen := len(followerList)

	for idx := range followerList {
		targetFollower := followerList[idx]
		followerOffset := <-targetFollower.WaitChan
		log.Println("33333333333333333333333", followerOffset, currentOffset)
		if followerOffset == currentOffset {
			listLen -= 1
		}
	}

	if listLen == 0 {
		return nil
	}

	return fmt.Errorf("FollowerManager WaitOffset failed: [%s - %d - %d]\n",
		topicName, partitionIndex, currentOffset)
}
