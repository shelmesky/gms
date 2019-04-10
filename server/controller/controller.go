package controller

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/shelmesky/gms/server/common"
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
				fmt.Printf("[%s] %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			case clientv3.EventTypeDelete:
				fmt.Printf("[%s] %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}
}

func HandleLeaderChange(leaderChan <-chan bool) {
	for leader := range leaderChan {
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
