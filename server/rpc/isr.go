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
		follower.WaitChan = make(chan int, 10240)
		follower.MessageChan = make(chan int, 10240)
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
		// TODO: follower一直向WaitChan放入offset， 导致填满WaitChan阻塞follower而引发follower不能同步消息
		// TODO: 这会发生在follower正常工作，但由于落后leader太多消息时发生，且这时leader又没有读取WaitChan(没有producer发送消息)
		// TODO: 应该使用超时判断( 或者len(WaitChan)判断元素长度? )，当阻塞时follower代替leader读取旧的offset.(一口气读取到剩余一半空间)
		// TODO: 因为旧的offset对于leader无用， leader只判断最新的. 或者说leader只会读取waitOffset时放入的那个offset.
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
			// #TODO: 如果这里通知了follower， 但是没有follower读取
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
			// #TODO: 上面follower没有读取MessageChan，就会导致WaitChan为空，读取阻塞
			// #TODO: 应使用时间轮超时读取，跳出循环
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
