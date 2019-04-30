package rpc

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/partition"
	"github.com/shelmesky/gms/server/topics"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"net"
	"strconv"
	"time"
)

const (
	SYNC         = 0
	SET_SYNC     = 1
	SYNC_MANAGER = 2
	CREATE_TOPIC = 3
)

// 所有RPC服务的请求头
type RPCRequest struct {
	Action   int
	Version  int
	Meta     RPCMeta
	Checksum []byte
}

// 所有RPC服务的返回值
type RPCReply struct {
	Code   int
	Result string
	Meta   RPCMeta
	Data   []byte
}

type RPCMeta struct {
	HW       uint64
	Revision int
}

// 分配给单个node的分区和副本信息
type NodePartitionReplicaInfo struct {
	NodeIndex      int    `json:"node_index"`
	NodeID         string `json:"node_id"`
	TopicName      string `json:"topic_name"`
	PartitionIndex int    `json:"partition_index"`
	ReplicaIndex   int    `json:"replica_index"`
	IsLeader       bool   `json:"is_leader"`
	HW             int    `json:"hw"`
}

func RPCHandleConnection(conn *net.TCPConn) {
	var err error

	log.Debugf("RPCHandleConnection() got client: %v\n", conn.RemoteAddr())

	for {
		var request RPCRequest

		decoder := gob.NewDecoder(conn)
		encoder := gob.NewEncoder(conn)

		/*
			err = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			if err != nil {
				log.Errorln("RPCHandleConnection() SetReadDeadline failed:", err)
				break
			}
		*/

		err = decoder.Decode(&request)

		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Println("RPCHandleConnection() read timeout:", err)
				continue
			} else {
				log.Warningln("RPCHandleConnection() decode request failed:", err)
				err = conn.Close()
				if err != nil {
					log.Errorln("RPCHandleConnection() close connection failed:", err)
				}
				break
			}
		}

		/*
			// 如果正常读取数据，则取消超时限制
			err = conn.SetReadDeadline(time.Time{})
			if err != nil {
				log.Errorln("RPCHandleConnection() SetReadDeadline failed:", err)
				break
			}

		*/

		// 集群内的同步请求
		if request.Action == SYNC {
			err = RPCHandle_SYNC(encoder, decoder, conn)
		}

		// controller发送的设置SYNC信息的请求
		if request.Action == SET_SYNC {
			err = RPCHandle_SET_SYNC(encoder, decoder, conn)
		}

		if request.Action == SYNC_MANAGER {
			err = RPCHandle_SYNC_MANAGER(encoder, decoder, conn)
		}

		// controller发送的创建topic的请求
		if request.Action == CREATE_TOPIC {
			err = RPCHandle_CREATE_TOPIC(encoder, decoder, conn)
		}

		if err != nil {
			log.Errorln("RPCHandleConnection() process action failed:", err)
			break
		}
	}
}

func RPCHandle_SYNC(encoder *gob.Encoder, decoder *gob.Decoder, conn *net.TCPConn) error {
	var follower Follower
	var buffer []byte
	var rpcReply RPCReply

	err := decoder.Decode(&follower)

	if err != nil {
		err = errors.Wrap(err, "RPCHandle_SYNC() Decode failed")
		return err
	}

	log.Debugln("RPCHandle_SYNC() receive follower:", follower)

	client := common.NewClient(conn)

	partitionIndex := strconv.Itoa(follower.PartitionIndex)
	offset := uint32(follower.Offset)
	count := uint32(follower.Count)

	// 将当前follower的RPC请求信息发送到统计HW的channel
	var offsetEntry OffsetEntry

	tempTopic := topics.TopicManager.GetTopic(follower.TopicName)
	if tempTopic != nil {
		tempPartition := tempTopic.GetPartition(follower.PartitionIndex)
		if tempPartition != nil {
			offsetEntry.TopicName = follower.TopicName
			offsetEntry.PartitionIdx = follower.PartitionIndex
			offsetEntry.ReplicaIdx = follower.Replica
			offsetEntry.Offset = follower.Offset
			offsetEntry.ReplicaNum = tempPartition.ReplicasNum

			err = PartitionHWManager.Add(offsetEntry)
			if err != nil {
				log.Errorln("RPCHandle_SYNC() -> PartitionHWManager.Add() failed:", err)
			}
		}
	}

	hw := PartitionHWManager.GetPartitionHW(follower.TopicName, follower.PartitionIndex)
	log.Println("&&&&&&&&&&&&&&&&&&&&&& RPCHandle_SYNC() GetPartitionHW:", hw)
	rpcReply.Meta.HW = hw

	/*
		通过topic名称， 分区编号， 消息的offset查找在SYNC_MANAGER阶段添加到Follower Manager的follower，
		这个最早的follower才是管理每个follower的真正对象，
		同时还会判断当前follower请求的offset是否小于分区的currentOffset， 如果小于不用等待MessageChannel，直接读取。

		如果读取ReadMessage失败， follower会重新发起RPC SYNC命令，带着上次同样的offset， 并不会丢失消息。
	*/
	followerSmallOffset := false
	targetFollower := GlobalFollowerManager.Get(follower, false)
	if targetFollower != nil {
		topic := topics.TopicManager.GetTopic(follower.TopicName)
		if topic != nil {
			Partition := topic.GetPartition(follower.PartitionIndex)
			if Partition != nil {
				if Partition.GetCurrentOffset() > follower.Offset {
					followerSmallOffset = true
				}
			}
		}

		if !followerSmallOffset {
			select {
			// 检测2秒钟内是否有新消息，否则返回给RPC客户端超时
			case <-common.GlobalTimingWheel.After(2000 * time.Millisecond):
				rpcReply.Code = 1
				rpcReply.Result = "RPCHandle_SYNC() Wait MessageChan timeout..."
				rpcReply.Data = []byte{}

				err = encoder.Encode(rpcReply)
				if err != nil {
					log.Errorln(errors.Wrap(err, "RPCHandle_SYNC() Encode failed"))
				}

				// 返回nil，上层的for循环会继续运行，RPC连接不会关闭
				return nil

			case messageOffset := <-targetFollower.MessageChan:
				log.Debugln("read <-targetFollower.MessageChan ", messageOffset, follower.Offset)
				break
			}
		}
	} else {
		return fmt.Errorf("RPCHandle_SYNC() -> GlobalFollowerManager.Get() is nil\n")
	}

	GlobalFollowerManager.String()

	// 通过topic名称， 分区编号， 消息的offset和数量发送读取消息给客户端
	// 这里并没有通过RPC的方式读取，而是直接将对应的文件内容通过sendfile系统调用发送

	n, buffer, err := topics.ReadMessage(&client, follower.TopicName, partitionIndex, offset, count, false)
	// 即使想要读取的offset和当前分区的offset一样，也会返回一个消息，所以此处n必然大于0

	log.Println("ReadMessage() return result:", n, len(buffer), err)

	if err == nil {
		rpcReply.Code = 0
		rpcReply.Result = "OK"
		rpcReply.Data = buffer

		err = GlobalFollowerManager.PutOffset(follower, true)
		if err != nil {
			rpcReply.Code = 2
			rpcReply.Result = "ReadMessage() reads empty message"
			rpcReply.Data = []byte{}

			log.Println("GlobalFollowerManager.PutOffset failed:", follower, err)
		}

	} else {
		rpcReply.Code = 3
		rpcReply.Result = err.Error()
		rpcReply.Data = buffer

		log.Errorln("ReadMessage() failed:", follower, err)

	}

	err = encoder.Encode(rpcReply)
	if err != nil {
		log.Errorln(errors.Wrap(err, "RPCHandle_SYNC() Encode failed"))
	}

	return nil

}

func RPCHandle_SET_SYNC(encoder *gob.Encoder, decoder *gob.Decoder, conn *net.TCPConn) error {
	var reply RPCReply
	var setSyncInfo SetSYNCInfo
	var err error

	/*
		err = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		if err != nil {
			return errors.Wrap(err, "RPCHandle_SET_SYNC() SetReadDeadline failed:")
		}
	*/

	err = decoder.Decode(&setSyncInfo)

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return errors.Wrap(err, "RPCHandle_SET_SYNC() read timeout:")
		} else {
			log.Warningln("RPCHandle_SET_SYNC() decode SetSYNCInfo failed:", err)
			err = conn.Close()
			if err != nil {
				log.Errorln("RPCHandle_SET_SYNC() close connection failed:", err)
			}
			return err
		}
	}

	/*
		// 如果正常读取数据，则取消超时限制
		err = conn.SetReadDeadline(time.Time{})
		if err != nil {
			return errors.Wrap(err, "RPCHandle_SET_SYNC() SetReadDeadline failed:")
		}
	*/

	// 检查参数是否提供
	if len(setSyncInfo.TopicName) == 0 {
		reply.Code = 1
		errTemp := fmt.Errorf("RPCHandle_SET_SYNC() setSyncInfo topic is empty, syncinfo: [%v]\n", setSyncInfo)
		reply.Result = errTemp.Error()

		err = encoder.Encode(reply)
		if err != nil {
			log.Warningln("RPCHandle_SET_SYNC() Encode RPCReply failed:", err)
			err = conn.Close()
			if err != nil {
				log.Errorln("RPCHandle_SET_SYNC() close connection failed:", err)
			}
			return err
		}

		return errTemp
	}

	log.Println("RPCHandle_SET_SYNC() got set sync info:", setSyncInfo)
	manageChan := FollowerStartSync(setSyncInfo)

	value := <-manageChan
	if errTemp, ok := value.(error); ok {
		err = errTemp
	} else {
		err = nil
	}

	if err != nil {
		reply.Code = 2
		reply.Result = fmt.Sprintf("RPCHandle_SET_SYNC() start sync for [%v] failed: %s\n",
			setSyncInfo, err.Error())
	} else {
		reply.Code = 0
		reply.Result = "OK"
	}

	err = encoder.Encode(reply)
	if err != nil {
		log.Warningln("RPCHandle_SET_SYNC() Encode RPCReply failed:", err)
		err = conn.Close()
		if err != nil {
			log.Errorln("RPCHandle_SET_SYNC() close connection failed:", err)
		}
		return err
	}

	return nil
}

// 处理由controller发送来的， 让每个leader开启Follower Manager的SYNC_MANAGER命令
func RPCHandle_SYNC_MANAGER(encoder *gob.Encoder, decoder *gob.Decoder, conn *net.TCPConn) error {
	var syncManager SetSYNCManager
	var err error
	var reply RPCReply

	err = decoder.Decode(&syncManager)

	if err != nil {
		log.Warningln("RPCHandle_SYNC_MANAGER() decode NodePartitionReplicaInfo failed:", err)
		err = conn.Close()
		if err != nil {
			log.Errorln("RPCHandle_SYNC_MANAGER() close connection failed:", err)
		}
		return err
	}

	// 计算当前分区副本的数量并保存在partition.Partition.ReplicasNum
	// 这是进程刚启动时第一次保存这个值， 之后当节点离线时会更新这个值。
	topicName := syncManager.TopicName
	partitionIdx := syncManager.PartitionIndex
	key := fmt.Sprintf("/topics-brokers/%s/partition-%d/", topicName, partitionIdx)
	getResp, err := common.ETCDGetKey(key, true)
	if err == nil {
		replicasNum := len(getResp.Kvs)
		if replicasNum > 0 {
			topic := topics.TopicManager.GetTopic(topicName)
			if topic != nil {
				tempPartition := topic.GetPartition(partitionIdx)
				// 设置一个分区的follower副本数量为所有副本数量-1(减去leader)
				tempPartition.ReplicasNum = replicasNum - 1
				log.Printf("RPCHandle_SYNC_MANAGER() set [%s -> partition-%d -> replica-%d] replicasNum to %d\n",
					topicName, partitionIdx, syncManager.ReplicaIndex, tempPartition.ReplicasNum)
			}
		}
	}

	var follower Follower
	follower.TopicName = syncManager.TopicName
	follower.PartitionIndex = syncManager.PartitionIndex
	follower.Replica = syncManager.ReplicaIndex

	// 将follower的信息加入Follower Manager
	GlobalFollowerManager.Add(follower)
	log.Println("RPCHandle_SYNC_MANAGER() got SYNC_MANAGER:", syncManager)

	reply.Code = 0
	reply.Result = "OK"

	err = encoder.Encode(reply)
	if err != nil {
		log.Warningln("RPCHandle_SYNC_MANAGER() Encode RPCReply failed:", err)
		err = conn.Close()
		if err != nil {
			log.Errorln("RPCHandle_SYNC_MANAGER() close connection failed:", err)
		}
		return err
	}

	return nil
}

func RPCHandle_CREATE_TOPIC(encoder *gob.Encoder, decoder *gob.Decoder, conn *net.TCPConn) error {
	var nodeParRepInfo NodePartitionReplicaInfo
	var err error
	var reply RPCReply

	err = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	if err != nil {
		return errors.Wrap(err, "RPCHandle_CREATE_TOPIC() SetReadDeadline failed:")
	}

	err = decoder.Decode(&nodeParRepInfo)

	if err != nil {
		log.Warningln("RPCHandle_CREATE_TOPIC() decode NodePartitionReplicaInfo failed:", err)
		err = conn.Close()
		if err != nil {
			log.Errorln("RPCHandle_CREATE_TOPIC() close connection failed:", err)
		}
		return err
	}

	// 如果正常读取数据，则取消超时限制
	err = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return errors.Wrap(err, "RPCHandle_CREATE_TOPIC() SetReadDeadline failed:")
	}

	reply, err = createTopic(&nodeParRepInfo)
	if err != nil {
		log.Warningln("RPCHandle_CREATE_TOPIC() createTopic() failed:", err)
		err = conn.Close()
		if err != nil {
			log.Errorln("RPCHandle_CREATE_TOPIC() close connection failed:", err)
		}
		return err
	}

	err = encoder.Encode(reply)
	if err != nil {
		log.Warningln("RPCHandle_CREATE_TOPIC() Encode RPCReply failed:", err)
		err = conn.Close()
		if err != nil {
			log.Errorln("RPCHandle_CREATE_TOPIC() close connection failed:", err)
		}
		return err
	}

	return nil
}

func createTopic(arg *NodePartitionReplicaInfo) (RPCReply, error) {
	var err error
	var reply RPCReply

	err = partition.CreatePartition(arg.TopicName, arg.PartitionIndex)
	if err != nil {
		reply.Code = 1
		reply.Result = fmt.Sprintf("CreatePartition failed: %s\n", err.Error())
		return reply, err
	}

	reply.Code = 0
	reply.Result = "OK"

	return reply, nil
}

func SendCreatTopic(nodeAddress string, nodePort int, arg NodePartitionReplicaInfo) error {
	var request RPCRequest

	port := strconv.Itoa(nodePort)
	target := nodeAddress + ":" + port

	conn, err := net.DialTimeout("tcp", target, time.Second*3)
	if err != nil {
		return err
	}

	encoder := gob.NewEncoder(conn)

	request.Action = CREATE_TOPIC
	request.Version = common.VERSION

	err = encoder.Encode(request)
	if err != nil {
		err = conn.Close()
		if err != nil {
			log.Errorln("SendCreatTopic() close connection failed:", err)
		}

		return err
	}

	err = encoder.Encode(arg)
	if err != nil {
		err = conn.Close()
		if err != nil {
			log.Errorln("SendCreatTopic() close connection failed:", err)
		}

		return err
	}

	return nil
}

// 由controller通知其他follower节点的同步信息
type SetSYNCInfo struct {
	TopicName      string
	PartitionIndex int
	ReplicaIndex   int
	LeaderAddress  string
	LeaderNodeID   string
}

// controller发送给leader节点启动follower manager
type SetSYNCManager struct {
	TopicName      string
	PartitionIndex int
	ReplicaIndex   int
}

/*
 list中保存的是etcd中topic的列表
 根据分区x副本的数量得到所有副本列表
 再将这些信息通知需要向leader副本同步的节点
*/
func SendSetSYNC(topicList []*mvccpb.KeyValue) error {
	var topic common.Topic
	var err error

	if len(topicList) == 0 {
		return fmt.Errorf("empty topics list.")
	}

	// 循环处理所有topic
	for idx := range topicList {
		kv := topicList[idx]
		valueBytes := kv.Value
		err = json.Unmarshal(valueBytes, &topic)
		if err != nil {
			return err
		}

		// 根据topic的分区数量，获取分区下的副本列表
		for i := 0; i < int(topic.PartitionCount); i++ {
			key := fmt.Sprintf("/topics-brokers/%s/partition-%d/", topic.TopicName, i)
			getResp, err := common.ETCDGetKey(key, true)
			if err != nil {
				return errors.Wrap(err, "SendSYNCSet() call ETCDGetKey() failed")
			}

			// 找到副本列表中的leader，并将leader副本的信息发送给此分区的其他副本节点
			var leaderID string
			var nodeParRepInfo NodePartitionReplicaInfo
			var syncInfo SetSYNCInfo
			for j := 0; j < len(getResp.Kvs); j++ {
				kv := getResp.Kvs[j]
				valueBytes := kv.Value
				err = json.Unmarshal(valueBytes, &nodeParRepInfo)
				if err != nil {
					return err
				}

				// 找到分区的副本列表中，leader的ID
				if nodeParRepInfo.IsLeader {
					leaderID = nodeParRepInfo.NodeID
				}

				// 如果不是leader节点，则将leader信息发送给这些follower节点
				if !nodeParRepInfo.IsLeader {
					// 生成发送SET_SYNC信息给follower信息
					syncInfo.TopicName = topic.TopicName
					syncInfo.PartitionIndex = i
					syncInfo.ReplicaIndex = nodeParRepInfo.ReplicaIndex

					leaderNode, err := common.GetSingleNode(leaderID)
					if err != nil {
						return err
					}
					portStr := strconv.Itoa(leaderNode.RPCPort)
					syncInfo.LeaderAddress = leaderNode.IPAddress + ":" + portStr
					syncInfo.LeaderNodeID = leaderID

					// 获取follower节点的信息
					followerNodeID := nodeParRepInfo.NodeID
					followerNode, err := common.GetSingleNode(followerNodeID)
					if err != nil {
						return err
					}

					// 发送SET_SYNC信息给follower
					followerAddr := followerNode.IPAddress + ":" + strconv.Itoa(followerNode.RPCPort)
					conn, err := net.DialTimeout("tcp", followerAddr, time.Second*3)
					if err != nil {
						return err
					}

					encoder := gob.NewEncoder(conn)

					var request RPCRequest
					request.Version = common.VERSION
					request.Action = SET_SYNC

					err = encoder.Encode(request)
					if err != nil {
						err = conn.Close()
						if err != nil {
							log.Errorln("SendSYNCSet() close connection failed:", err)
						}
						return err
					}

					err = encoder.Encode(syncInfo)
					if err != nil {
						err = conn.Close()
						if err != nil {
							log.Errorln("SendSYNCSet() close connection failed:", err)
						}
						return err
					}

					// 获取RPC响应结果
					var reply RPCReply
					decoder := gob.NewDecoder(conn)
					err = decoder.Decode(&reply)
					if err != nil {
						err = conn.Close()
						if err != nil {
							log.Errorln("SendSYNCSet() close connection failed:", err)
						}
						return err
					}

					log.Debugln("SendSYNCSet() got reply from rpc server:", reply)

					// 将SYNC_MANAGER命令(包含follower的信息)发送给leader， Leader会启动Follower manager
					leaderAddr := followerNode.IPAddress + ":" + strconv.Itoa(leaderNode.RPCPort)
					conn, err = net.DialTimeout("tcp", leaderAddr, time.Second*3)
					if err != nil {
						return err
					}

					leaderEncoder := gob.NewEncoder(conn)
					leaderDecoder := gob.NewDecoder(conn)

					var syncManager SetSYNCManager
					syncManager.TopicName = topic.TopicName
					syncManager.PartitionIndex = i
					syncManager.ReplicaIndex = nodeParRepInfo.ReplicaIndex

					request.Version = common.VERSION
					request.Action = SYNC_MANAGER

					err = leaderEncoder.Encode(request)
					if err != nil {
						err = conn.Close()
						if err != nil {
							log.Errorln("SendSYNCSet() close connection failed:", err)
						}
						return err
					}

					err = leaderEncoder.Encode(syncManager)
					if err != nil {
						err = conn.Close()
						if err != nil {
							log.Errorln("SendSYNCSet() close connection failed:", err)
						}
						return err
					}

					// 获取RPC响应结果
					err = leaderDecoder.Decode(&reply)
					if err != nil {
						err = conn.Close()
						if err != nil {
							log.Errorln("SendSYNCSet() close connection failed:", err)
						}
						return err
					}

					log.Debugln("SendSYNCSet() got reply from rpc server:", reply)
				}
			}
		}
	}

	return nil
}
