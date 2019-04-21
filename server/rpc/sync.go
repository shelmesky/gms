package rpc

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/topics"
	log "github.com/sirupsen/logrus"
	"io"
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
	Count          int
}

// 连接到leader副本的RPC服务， 并持续同步内容
func FollowerStartSync(info SetSYNCInfo) chan interface{} {
	manageChan := make(chan interface{}, 16)

	go func(manageChan chan interface{}) {

		var syncLeader SyncLeader
		var request RPCRequest
		//var reply RPCReply

		syncLeader.NodeID = common.GlobalConfig.NodeID
		syncLeader.NodeAddress = common.GlobalConfig.IPAddress
		syncLeader.TopicName = info.TopicName
		syncLeader.PartitionIndex = info.PartitionIndex

		// 找到topic
		topic := topics.TopicManager.GetTopic(info.TopicName)

		if topic == nil {
			manageChan <- fmt.Errorf("FollowerStartSync() can not find any topic [%s]\n", info.TopicName)
		}

		targetLeader := info.Leader

		// 连接到服务器， 获取发送SYNC命令
		conn, err := net.DialTimeout("tcp", targetLeader, time.Second*5)
		if err != nil {
			err = errors.Wrap(err, "FollowerStartSync() dial failed")
			manageChan <- err
		}

		encoder := gob.NewEncoder(conn)
		//decoder := gob.NewDecoder(conn)

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
				// 找到partition
				partitionObject := topic.GetPartition(info.PartitionIndex)

				// 告诉server当前自己的offset
				syncLeader.Offset = partitionObject.GetCurrentOffset() + 1
				//syncLeader.Offset = tempOffset
				syncLeader.Count = 1

				request.Action = SYNC
				request.Version = common.VERSION

				// 发送request
				err = encoder.Encode(request)

				if err != nil {
					log.Println("FollowerStartSync() Encode failed:", err)
					err = conn.Close()
					if err != nil {
						log.Println("FollowerStartSync() close connection failed:", err)
						err = errors.Wrap(err, "FollowerStartSync() close connection failed: %s\n")
						manageChan <- err
					}

					time.Sleep(2 * time.Second)
					continue
				}

				// 发送同步信息
				err = encoder.Encode(syncLeader)
				if err != nil {
					log.Println("FollowerStartSync() Encode failed:", err)
					err = conn.Close()
					if err != nil {
						log.Println("FollowerStartSync() close connection failed:", err)
						err = errors.Wrap(err, "FollowerStartSync() close connection failed:")
						manageChan <- err
					}

					time.Sleep(2 * time.Second)
					continue
				}

				/*
					// 读取返回数据
					err = decoder.Decode(&reply)
					if err != nil {

						log.Println("FollowerStartSync() Deocde failed:", err)

						time.Sleep(2 * time.Second)
						manageChan <- err
						continue
					}

					// 服务器读取时发生错误
					if reply.Code != 0 {
						log.Errorln("FollowerStartSync() receive reply from server:", reply.Result)
						if reply.Code == 1 {
							// 没有新的消息， 暂停2秒
							time.Sleep(2 * time.Second)
						}
					} else {
						log.Debugln("FollowerStartSync() start read data from server")*/

				count := 0

				//未发生错误，使用字节数组的方式读取
				for {
					offsetBuf := make([]byte, 4)
					lengthBuf := make([]byte, 4)

					err = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
					if err != nil {
						log.Println(errors.Wrap(err, "RPCHandle_SET_SYNC() SetReadDeadline failed:"))
						break
					}

					readN, err := io.ReadFull(conn, offsetBuf)
					if readN == 0 || err != nil {
						break
					}

					// 如果正常读取数据，则取消超时限制
					err = conn.SetReadDeadline(time.Time{})
					if err != nil {
						log.Println(errors.Wrap(err, "RPCHandle_SET_SYNC() SetReadDeadline failed:"))
						break
					}

					offset := binary.LittleEndian.Uint32(offsetBuf)

					readN, err = io.ReadFull(conn, lengthBuf)
					if readN == 0 || err != nil {
						break
					}

					length := binary.LittleEndian.Uint32(lengthBuf)

					log.Printf("###### offset: %d, length: %d\n", offset, length)

					bodyBuf := make([]byte, length)

					readN, err = io.ReadFull(conn, bodyBuf)
					if readN == 0 || err != nil {
						break
					}

					body := common.BytesToMessage(bodyBuf)
					log.Println("###### body: ", body)

					key := bodyBuf[common.WRITE_MESSAGE_LEN : common.WRITE_MESSAGE_LEN+body.KeyLength]
					value := bodyBuf[common.WRITE_MESSAGE_LEN+body.KeyLength : common.WRITE_MESSAGE_LEN+body.KeyLength+body.ValueLength]
					log.Println("###### key: ", string(key))
					log.Println("###### value: ", string(value))

					diskLog := partitionObject.GetLog()
					n, err := diskLog.AppendBytes(bodyBuf, int(length))
					if n != int(length) {
						log.Errorf("FollowerStartSync() AppendBytes to disk failed: [written -> %d != bodyLength -> %d]\n",
							n, length)
					}

					log.Debugf("FollowerStartSync() written [%d] bytes to disk.\n", n)

					count += 1
					if count == syncLeader.Count {
						break
					}
					//}
				}
			}
		}
	}(manageChan)

	return manageChan
}
