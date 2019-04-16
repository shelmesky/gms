package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/node"

	//	"github.com/shelmesky/gms/server/server"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"os"
	"time"
)

var (
	electionName     = "/controller-election"
	resumeLeader     = true
	TTL              = 10
	reconnectBackOff = time.Second * 2
	session          *concurrency.Session
	election         *concurrency.Election
	client           *etcd.Client
	isController     = false
)

func Start() {
	var err error

	log.SetLevel(log.InfoLevel)

	client, err = etcd.New(etcd.Config{
		Endpoints: []string{common.GlobalConfig.EtcdServer},
	})
	if err != nil {
		log.Println(errors.Wrap(err, "connect to etcd failed"))
		os.Exit(1)
	}

	ctx, _ := context.WithCancel(context.Background())
	leaderChan, err := runElection(ctx)
	if err != nil {
		log.Println(errors.Wrap(err, "run election failed"))
		os.Exit(1)
	}

	// 处理controller发生变化
	go HandleLeaderChange(leaderChan)

	go WatchBrokers()
	go WatchTopics()

	//cancel()
}

// 监听/brokers/id/目录的变化并处理，例如新增或删除
func WatchBrokers() {
	var err error

	// watcher中使用新的etcd连接
	client, err = etcd.New(etcd.Config{
		Endpoints: []string{common.GlobalConfig.EtcdServer},
	})
	if err != nil {
		log.Println(errors.Wrap(err, "connect to etcd failed"))
		os.Exit(1)
	}

	watcher := clientv3.NewWatcher(client)
	watchChan := watcher.Watch(context.Background(), "/brokers/ids/", clientv3.WithPrefix())
	for wresp := range watchChan {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				fmt.Printf("[%s] %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			case clientv3.EventTypeDelete:
				fmt.Printf("[%s] %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}
}

// 监听/topics/目录并处理，例如新增或删除
func WatchTopics() {
	var err error

	client, err = etcd.New(etcd.Config{
		Endpoints: []string{common.GlobalConfig.EtcdServer},
	})
	if err != nil {
		log.Println(errors.Wrap(err, "connect to etcd failed"))
		os.Exit(1)
	}

	watcher := clientv3.NewWatcher(client)
	watchChan := watcher.Watch(context.Background(), "/topics/", clientv3.WithPrefix())
	for wresp := range watchChan {
		for _, ev := range wresp.Events {
			switch ev.Type {

			case clientv3.EventTypePut:
				// 创建topic
				fmt.Printf("[%s] %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				err = ControllerSendCreateTopic(ev.Kv.Key, ev.Kv.Value)
				if err != nil {
					log.Printf("create topic [%q] failed: %v\n", ev.Kv.Key, err)
				} else {
					log.Printf("create topic [%q] success!\n", ev.Kv.Key)
				}

			case clientv3.EventTypeDelete:
				// 删除topic
				fmt.Printf("[%s] %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}
}

// 分区和副本的数量总和， 例如3个分区， 每个分区3副本， 则总共3x3个副本分区
// 将这些'分区副本'按照集群内节点的数量排序依次分配
// nodeCount: 节点的数量
// allReplicaCount: 所有分区的副本数量
func generateDisList(nodeCount int, allReplicaCount int) []int {
	var disList []int
	var nodeTemp []int

	for i := 0; i < nodeCount; i++ {
		nodeTemp = append(nodeTemp, i)
	}

	idx := 0
	startIdx := 0
	round := false
	for j := 0; j < allReplicaCount; j++ {

		if idx == nodeCount {
			idx = 0
		}

		if len(disList) > 0 && len(disList)%nodeCount == 0 {
			round = true
		}
		if round == true {
			startIdx += 1
			if startIdx == nodeCount {
				startIdx = 0
			}
			idx = startIdx
			round = false
		}

		value := nodeTemp[idx]
		disList = append(disList, value)

		idx += 1
	}

	return disList
}

/*
Controller发送创建topic的指令给各node.
1. 查询当前节点是不是controller， 不是则不处理
2. 在etcd中查询当前的节点数量
3. 如果节点数量小于replica数量则不创建
4. 根据节点的数量， 按照partition的排序，以此将创建副本的命令发送给node
*/
func ControllerSendCreateTopic(key, value []byte) error {
	if !isController {
		return fmt.Errorf("current node is not controller, just return.")
	}

	var err error
	var client *clientv3.Client

	var topicInfo common.Topic

	err = json.Unmarshal(value, &topicInfo)
	if err != nil {
		return err
	}

	client, err = clientv3.New(clientv3.Config{
		Endpoints: []string{common.GlobalConfig.EtcdServer},
	})

	if err != nil {
		return fmt.Errorf("%s: connect to kv server failed\n", err)
	}

	kv := clientv3.NewKV(client)

	allNodeKey := "/brokers/ids/"
	getResp, err := kv.Get(context.Background(), allNodeKey, clientv3.WithPrefix())

	if err != nil {
		return fmt.Errorf("%s: get %s from etcd failed\n", err, allNodeKey)
	}

	// 获取node数量
	nodeNum := len(getResp.Kvs)
	// 如果node
	if uint32(nodeNum) < topicInfo.ReplicaCount {
		return fmt.Errorf("replica count bigger than num of node.")
	}

	var nodeList []*node.Node

	for i := 0; i < nodeNum; i++ {
		nodeInfo := new(node.Node)
		err = json.Unmarshal(getResp.Kvs[i].Value, nodeInfo)
		if err == nil {
			nodeList = append(nodeList, nodeInfo)
		}
	}

	type nodePartitionReplica struct {
		nodeIndex      int
		nodeID         string
		partitionIndex int
		replicaIndex   int
	}

	var nodeParRepList []*nodePartitionReplica

	// 依次将分区和副本分配到每个节点上
	for m := 0; m < int(topicInfo.PartitionCount); m++ {
		for n := 0; n < int(topicInfo.ReplicaCount); n++ {
			item := new(nodePartitionReplica)
			item.partitionIndex = m
			item.replicaIndex = n
			nodeParRepList = append(nodeParRepList, item)
		}
	}

	list := generateDisList(nodeNum, int(topicInfo.PartitionCount*topicInfo.ReplicaCount))

	for idx := range nodeParRepList {
		nodeParRepList[idx].nodeIndex = list[idx]
		node := nodeList[list[idx]]
		nodeParRepList[idx].nodeID = node.NodeID
	}

	for x := range nodeParRepList {
		fmt.Println("aaaaaaaaa", nodeParRepList[x])
	}

	return nil
}

func HandleLeaderChange(leaderChan <-chan bool) {
	for leader := range leaderChan {
		if leader == true {
			isController = true
		} else {
			isController = false
		}
		fmt.Printf("Leader: %t\n", leader)
	}
}

func runElection(ctx context.Context) (<-chan bool, error) {
	var observe <-chan etcd.GetResponse
	var node *etcd.GetResponse
	var errChan chan error
	var isLeader bool
	var err error

	var leaderChan chan bool
	setLeader := func(set bool) {
		// Only report changes in leadership
		if isLeader == set {
			return
		}
		isLeader = set
		leaderChan <- set
	}

	if err = newSession(ctx, 0); err != nil {
		return nil, errors.Wrap(err, "while creating initial session")
	}

	go func() {
		leaderChan = make(chan bool, 10)
		defer close(leaderChan)

		for {

			// 首先查找目前的leader，如果出错则处理
			if node, err = election.Leader(ctx); err != nil {
				// 如果错误是找不到leader，则不做任何处理，否则重新连接
				if err != concurrency.ErrElectionNoLeader {
					log.Errorf("while determining election leader: %s", err)
					goto reconnect
				}
			} else { //如果找到了leader信息
				// If we are resuming an election from which we previously had leadership we
				// have 2 options
				// 1. Resume the leadership if the lease has not expired. This is a race as the
				//    lease could expire in between the `Leader()` call and when we resume
				//    observing changes to the election. If this happens we should detect the
				//    session has expired during the observation loop.
				// 2. Resign the leadership immediately to allow a new leader to be chosen.
				//    This option will almost always result in transfer of leadership.
				if string(node.Kvs[0].Value) == common.GlobalConfig.NodeID { //如果leader是自身
					// If we want to resume leadership
					if resumeLeader { // 如果打开了恢复leader的开关
						// Recreate our session with the old lease id
						// 使用旧的lease id创建session
						if err = newSession(ctx, node.Kvs[0].Lease); err != nil {
							log.Errorf("while re-establishing session with lease: %s", err)
							goto reconnect
						}
						election = concurrency.ResumeElection(session, electionName,
							string(node.Kvs[0].Key), node.Kvs[0].CreateRevision)

						// Because Campaign() only returns if the election entry doesn't exist
						// we must skip the campaign call and go directly to observe when resuming
						goto observe
					} else {
						// If resign takes longer than our TTL then lease is expired and we are no
						// longer leader anyway.
						ctx, cancel := context.WithTimeout(ctx, time.Duration(TTL)*time.Second)
						election := concurrency.ResumeElection(session, electionName,
							string(node.Kvs[0].Key), node.Kvs[0].CreateRevision)
						err = election.Resign(ctx)
						cancel()
						if err != nil {
							log.Errorf("while resigning leadership after reconnect: %s", err)
							goto reconnect
						}
					}
				}
			}
			// Reset leadership if we had it previously
			setLeader(false)

			// Attempt to become leader
			// 尝试成为leader
			errChan = make(chan error)
			go func() {
				// Make this a non blocking call so we can check for session close
				errChan <- election.Campaign(ctx, common.GlobalConfig.NodeID)
			}()

			select {
			case err = <-errChan:
				if err != nil {
					if errors.Cause(err) == context.Canceled {
						return
					}
					// NOTE: Campaign currently does not return an error if session expires
					log.Errorf("while campaigning for leader: %s", err)
					session.Close()
					goto reconnect
				}
			case <-ctx.Done():
				session.Close()
				return
			case <-session.Done():
				goto reconnect
			}

		observe:
			// If Campaign() returned without error, we are leader
			// 如果调用Campaign() 没有返回错误，我们就成为leader
			setLeader(true)

			// Observe changes to leadership
			// 观察leader的改变
			observe = election.Observe(ctx)
			for {
				select {
				case resp, ok := <-observe:
					if !ok {
						// NOTE: Observe will not close if the session expires, we must
						// watch for session.Done()
						session.Close()
						goto reconnect
					}
					if string(resp.Kvs[0].Value) == common.GlobalConfig.NodeID {
						setLeader(true)
					} else {
						// We are not leader
						setLeader(false)
						break
					}
				case <-ctx.Done():
					if isLeader {
						// If resign takes longer than our TTL then lease is expired and we are no
						// longer leader anyway.
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(TTL)*time.Second)
						if err = election.Resign(ctx); err != nil {
							log.Errorf("while resigning leadership during shutdown: %s", err)
						}
						cancel()
					}
					session.Close()
					return
				case <-session.Done():
					goto reconnect
				}
			}

		reconnect:
			setLeader(false)

			for {
				if err = newSession(ctx, 0); err != nil {
					if errors.Cause(err) == context.Canceled {
						return
					}
					log.Errorf("while creating new session: %s", err)
					tick := time.NewTicker(reconnectBackOff)
					select {
					case <-ctx.Done():
						tick.Stop()
						return
					case <-tick.C:
						tick.Stop()
					}
					continue
				}
				break
			}
		}
	}()

	// Wait until we have a leader before returning
	for {
		resp, err := election.Leader(ctx)
		if err != nil {
			if err != concurrency.ErrElectionNoLeader {
				return nil, err
			}
			time.Sleep(time.Millisecond * 300)
			continue
		}
		// If we are not leader, notify the channel
		if string(resp.Kvs[0].Value) != common.GlobalConfig.NodeID {
			leaderChan <- false
		}
		break
	}
	return leaderChan, nil
}

func newSession(ctx context.Context, id int64) error {
	var err error
	session, err = concurrency.NewSession(client, concurrency.WithTTL(TTL),
		concurrency.WithContext(ctx), concurrency.WithLease(etcd.LeaseID(id)))
	if err != nil {
		return err
	}
	election = concurrency.NewElection(session, electionName)
	return nil
}
