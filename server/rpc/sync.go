package rpc

import (
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/topics"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

// 向副本leader请求
type SyncLeader struct {
	NodeID         string
	NodeAddress    string
	TopicName      string
	PartitionIndex int
	Offset         int
}

// 连接到leader副本的RPC服务， 并持续同步内容
func FollowerStartSync(info SetSYNCInfo) chan interface{} {
	manageChan := make(chan interface{}, 16)

	go func(manageChan chan interface{}) {

		var syncLeader SyncLeader
		var request RPCRequest
		var reply RPCReply

		syncLeader.NodeID = common.GlobalConfig.NodeID
		syncLeader.NodeAddress = common.GlobalConfig.IPAddress
		syncLeader.TopicName = info.TopicName
		syncLeader.PartitionIndex = info.PartitionIndex

		topic := topics.TopicManager.GetTopic(info.TopicName)
		partition := topic.GetPartition(info.PartitionIndex)

		targetLeader := info.Leader

		conn, err := net.DialTimeout("tcp", targetLeader, time.Second*5)
		if err != nil {
			err = errors.Wrap(err, "FollowerStartSync() dial failed")
			manageChan <- err
		}

		encoder := gob.NewEncoder(conn)
		decoder := gob.NewDecoder(conn)

		log.Println("FollowerStartSync() start working for", info)
		manageChan <- nil

		for {
			select {
			case v := <-manageChan:
				if stopSignal, ok := v.(bool); ok {
					log.Warnf("FollowerStartSync() [%v] receive stop signal %v, quit.\n", info, stopSignal)
				}

				if newSetSyncInfo, ok := v.(SetSYNCInfo); ok {
					err := fmt.Errorf("FollowerStartSync() [%v] receive new leader: %v\n", info, newSetSyncInfo)
					manageChan <- err
				}

			default:

				syncLeader.Offset = partition.GetCurrentOffset()

				request.Action = SYNC
				request.Version = common.VERSION

				// 发送request
				err = encoder.Encode(request)
				if err != nil {
					err = conn.Close()
					if err != nil {
						err = errors.Wrap(err, "FollowerStartSync() close connection failed: %s\n")
						manageChan <- err
					}

					continue
				}

				// 发送同步信息
				err = encoder.Encode(syncLeader)
				if err != nil {
					err = conn.Close()
					if err != nil {
						err = errors.Wrap(err, "FollowerStartSync() close connection failed:")
						manageChan <- err
					}

					continue
				}

				// 读取返回数据
				err = decoder.Decode(&reply)
				if err != nil {
					manageChan <- err
					continue
				}

				log.Println("go SYNC reply from server:", string(reply.Data))
			}
		}
	}(manageChan)

	return manageChan
}
