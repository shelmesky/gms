package rpc

import (
	"encoding/json"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"sync"
	"time"
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

func (this *FollowerManager) Get(follower Follower, updateWriteTimestamp bool) *Follower {
	var ok bool
	var targetFollower *Follower

	this.RLock()
	defer this.RUnlock()

	key := this.makeKey(follower.TopicName, follower.PartitionIndex, follower.Replica)
	if targetFollower, ok = this.topicPartitionReplicaMap[key]; ok {

		this.UpdateISR(targetFollower, updateWriteTimestamp)

		return targetFollower
	}

	return nil
}

func (this *FollowerManager) UpdateISR(follower *Follower, updateWriteTimestamp bool) {
	now := time.Now().Unix()

	// 计算两次读写请求的时间差，用来评估follower同步的性能
	// updateWriteTimestamp只会在leader通知follower有消息时才会为true，无消息轮询时不会更新
	// 所以当producer没有新消息时，计算的是最近两次读写之间的事件差
	if updateWriteTimestamp {
		follower.LastWriteEnd = time.Now().UnixNano()
	}

	lastUpdate := follower.LastUpdate
	lastWriteStart := follower.LastWriteStart
	lastWriteEnd := follower.LastWriteEnd

	// 更新follower的最后请求时间
	follower.LastUpdate = time.Now().Unix()

	writeDuration := lastWriteEnd - lastWriteStart

	// 如果10秒钟内follower发起了第二次SYNC请求，
	// 并且follower两次读写请求的时间差大于0.01毫秒小于200毫秒，则认为是ISR状态
	if now-lastUpdate < 10 && writeDuration > 10000 && writeDuration < 200000000 {
		follower.IsISR = true
	} else {
		follower.IsISR = false
	}

	// 更新在etcd中某个follower的is_isr标志， 这个标志在副本leader离线发生leader选举时用到
	err := this.UpdateETCISR(follower.TopicName, follower.PartitionIndex, follower.Replica, follower.IsISR)
	if err != nil {
		log.Println("FollowerManager() -> UpdateISR -> UpdateETCISR faield:", err)
	}
}

func (this *FollowerManager) UpdateETCISR(topicName string, partitionIdx, replicaIdx int, isISR bool) error {
	key := fmt.Sprintf("/topics-brokers/%s/partition-%d/replica-%d", topicName, partitionIdx, replicaIdx)

	getResp, err := common.ETCDGetKey(key, false)
	if err != nil {
		return err
	}

	if len(getResp.Kvs) == 0 {
		return fmt.Errorf("FollowerManager.UpdateETCISR() -> ETCDGetKey failed:", err)
	}

	valueBytes := getResp.Kvs[0].Value
	if len(valueBytes) == 0 {
		return fmt.Errorf("FollowerManager.UpdateETCISR() value from etcd is empty")
	}

	var topicParReplicaInfo NodePartitionReplicaInfo
	err = json.Unmarshal(valueBytes, &topicParReplicaInfo)
	if err != nil {
		return err
	}

	topicParReplicaInfo.IsISR = isISR
	jsonBytes, err := json.Marshal(topicParReplicaInfo)
	if err != nil {
		return err
	}

	_, err = common.ETCDPutKey(key, string(jsonBytes))
	if err != nil {
		return err
	}

	return nil
}

func (this *FollowerManager) PutOffset(follower Follower, updateWriteTimestamp bool) error {
	targetFollower := this.Get(follower, updateWriteTimestamp)
	if targetFollower != nil {
		/* follower一直向WaitChan放入offset， 导致填满WaitChan阻塞follower而引发follower不能同步消息
		这会发生在follower正常工作，但由于落后leader太多消息时发生，且这时leader又没有读取WaitChan(没有producer发送消息)
		应该使用超时判断( 或者len(WaitChan)判断元素长度? )，当阻塞时follower代替leader读取旧的offset.(一口气读取到剩余一半空间)
		因为旧的offset对于leader无用， leader只判断最新的. 或者说leader只会读取waitOffset时放入的那个offset.*/
		select {
		case <-common.GlobalTimingWheel.After(50 * time.Millisecond):
			chanLength := len(targetFollower.WaitChan)
			for i := 0; i < chanLength; i++ {
				<-targetFollower.WaitChan
			}
		case targetFollower.WaitChan <- follower.Offset:
			log.Println("##################### PufOffset:", follower)
		}

	} else {
		return fmt.Errorf("FollowerManager cant find %v\n", follower)
	}

	return nil
}

func (this *FollowerManager) WaitOffset(topicName string, partitionIndex, currentOffset int, ack int8) error {
	var ISRFollowerList []*Follower

	key := this.makeKey(topicName, partitionIndex, 0)

	this.RLock()
	defer this.RUnlock()

	// leader当前的currentOffset大于follow
	currentOffset -= 1

	for k, v := range this.topicPartitionReplicaMap {
		if strings.HasPrefix(k, key) {
			// 向MessageChan中放入最新offset， 告诉**所有**follower有新消息可以读取
			// #TODO: 如果这里通知了follower， 但是没有follower读取
			v.MessageChan <- currentOffset

			// 记录通知follower有新消息的时间戳
			v.LastWriteStart = time.Now().UnixNano()

			// 获取所有ISR状态的follower
			if v.IsISR == true {
				ISRFollowerList = append(ISRFollowerList, v)
			}
		}
	}

	// 如果ack为0, 因为ack默认是数字类型默认为0, 所以直接退出
	// 如果ack为1, 说明只需要leader一个副本的确认即可
	if ack == 0 || ack == 1 {
		return nil
	}

	// 到这里就仅支持ack为-1的选项，其他数字都不支持
	// 所以下面是ack为-1的处理流程
	if ack != -1 {
		return utils.UnSupportAckMode
	}

	/*
		下面会获取所有在ISR状态的follower节点和节点的数量，然后将数量与参数InSyncReplicas对比大小。
	*/
	neededIRSReplicas := 0

	ISRListLen := len(ISRFollowerList)

	// 如果实际的ISR的数量小于预设置的InSyncReplicas数量，则返回没有足够的副本错误给producer
	if ISRListLen < common.GlobalConfig.InSyncReplicas {
		return utils.NotEnoughReplicas
	}

	// 如果实际的ISR为0，则报告给producer
	if ISRListLen == 0 {
		return utils.PartitionNoISRList
	}

	// 如果实际的ISR同步副本数量大于预设置的InSyncReplicas，则只需要等待预先设置的数量即可
	if ISRListLen > common.GlobalConfig.InSyncReplicas {
		neededIRSReplicas = common.GlobalConfig.InSyncReplicas
	} else {
		neededIRSReplicas = ISRListLen
	}

	// 需要将实际需要确认的副本数减1，因为leader本身也是同步副本之一
	neededIRSReplicas -= 1

	timeoutFollower := 0

	// 循环全部ISR列表
	for idx := range ISRFollowerList {
		targetFollower := ISRFollowerList[idx]

		// 如果已经读取到了足够的offset，不需要循环全部ISR列表，直接跳出循环
		if neededIRSReplicas == 0 {
			break
		}

		/*
			因为follower调用PutOffset时，先处理完消息的必然先调用，
			所以WaitChan中先读取到的offset说明其follower先处理完毕
		*/

		// 在循环中多次读取WaitChan， 因为follower可能在其中放入多个offset
		// 当follower的offset落后leader时会发生这种情况
		for {
			/*上面follower没有读取MessageChan，就会导致WaitChan为空，读取阻塞
			应使用时间轮超时读取，跳出循环*/
			select {
			case <-common.GlobalTimingWheel.After(100 * time.Millisecond):
				timeoutFollower += 1
				goto skiploop
			case followerOffset := <-targetFollower.WaitChan:
				log.Println("##################### WaitOffset:", followerOffset, currentOffset)
				if followerOffset >= currentOffset {
					neededIRSReplicas -= 1
					goto skiploop
				}
			}
		}
	skiploop:
	}

	// 期望的offset全部被follower读取
	if neededIRSReplicas == 0 {
		return nil
	}

	// 期望的offset超时，即一个follower也没有
	// 情况发生在虽然获得了ISR的列表，但是准备获取offset之前, 这些follower全部停止了
	if neededIRSReplicas == timeoutFollower {
		return utils.ISRFollowerTimeout
	}

	//// 期望的offset被部分follower读取
	//if neededIRSReplicas < timeoutFollower {
	//
	//}

	// 最后的错误是
	return fmt.Errorf("WaitOffset() faield: not enough follower\n")
}

func (this FollowerManager) String() {
	for k, v := range this.topicPartitionReplicaMap {
		log.Debugf("[%s] -> [%v]\n", k, v)
	}
}
