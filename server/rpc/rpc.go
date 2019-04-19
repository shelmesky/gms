package rpc

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/partition"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"net"
	"strconv"
	"time"
)

const (
	SYNC         = 0
	SET_SYNC     = 2
	CREATE_TOPIC = 1
)

// 所有RPC服务的请求头
type RPCRequest struct {
	Action   int
	Version  int
	Checksum []byte
}

// 所有RPC服务的返回值
type RPCReply struct {
	Code   int
	Result string
	Data   []byte
}

// 分配给单个node的分区和副本信息
type NodePartitionReplicaInfo struct {
	NodeIndex      int    `json:"node_index"`
	NodeID         string `json:"node_id"`
	TopicName      string `json:"topic_name"`
	PartitionIndex int    `json:"partition_index"`
	ReplicaIndex   int    `json:"replica_index"`
	IsLeader       bool   `json:"is_leader"`
}

func RPCHandleConnection(conn *net.TCPConn) {
	var request RPCRequest
	var err error
	var reply RPCReply

	log.Debugf("RPCHandleConnection() got client: %v\n", conn.RemoteAddr())

	decoder := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)

	for {
		err = decoder.Decode(&request)

		if err != nil {
			log.Warningln("RPCHandleConnection() decode failed:", err)
			err = conn.Close()
			if err != nil {
				log.Errorln("RPCHandleConnection() close connection failed:", err)
			}
			break
		}

		// 集群内的同步请求
		if request.Action == SYNC {
		}

		// controller发送来的设置SYNC信息的请求
		if request.Action == SET_SYNC {
			var reply RPCReply
			var setSyncInfo SetSYNCInfo

			err := decoder.Decode(setSyncInfo)
			if err != nil {
				log.Warningln("RPCHandleConnection() decode failed:", err)
				err = conn.Close()
				if err != nil {
					log.Errorln("RPCHandleConnection() close connection failed:", err)
				}
				break
			}

			log.Println("got set sync info:", setSyncInfo)

			reply.Code = 0
			reply.Result = "OK"
			err = encoder.Encode(reply)
			if err != nil {
				log.Warningln("RPCHandleConnection() Encode() failed:", err)
				err = conn.Close()
				if err != nil {
					log.Errorln("RPCHandleConnection() close connection failed:", err)
				}
				break
			}
		}

		// controller发送来的创建topic的请求
		if request.Action == CREATE_TOPIC {
			var nodeParRepInfo NodePartitionReplicaInfo
			err = decoder.Decode(&nodeParRepInfo)

			if err != nil {
				log.Warningln("RPCHandleConnection() decode failed:", err)
				err = conn.Close()
				if err != nil {
					log.Errorln("RPCHandleConnection() close connection failed:", err)
				}
				break
			}

			reply, err = createTopic(&nodeParRepInfo)
			if err != nil {
				log.Warningln("RPCHandleConnection() createTopic() failed:", err)
				err = conn.Close()
				if err != nil {
					log.Errorln("RPCHandleConnection() close connection failed:", err)
				}
				break
			}

			err = encoder.Encode(reply)
			if err != nil {
				log.Warningln("RPCHandleConnection() Encode() failed:", err)
				err = conn.Close()
				if err != nil {
					log.Errorln("RPCHandleConnection() close connection failed:", err)
				}
				break
			}
		}
	}
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

// 由controller通知其他节点的同步信息
type SetSYNCInfo struct {
	TopicName      string
	PartitionIndex int
	Leader         string
}

// list中保存的是etcd中topic的列表
// 根据分区x副本的数量得到所有副本列表
// 再将这些信息通知需要向副本leader同步的节点
func SendSYNCInfo(topicList []*mvccpb.KeyValue) error {
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
				return errors.Wrap(err, "SendSYNCInfo() call ETCDGetKey() failed")
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
					leaderNode, err := common.GetSingleNode(leaderID)
					if err != nil {
						return err
					}
					portStr := strconv.Itoa(leaderNode.RPCPort)
					syncInfo.Leader = leaderNode.IPAddress + ":" + portStr

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
					request.Version = 1000
					request.Action = SET_SYNC

					err = encoder.Encode(request)
					if err != nil {
						err = conn.Close()
						if err != nil {
							log.Errorln("SendSYNCInfo() close connection failed:", err)
						}
						return err
					}

					err = encoder.Encode(syncInfo)
					if err != nil {
						err = conn.Close()
						if err != nil {
							log.Errorln("SendSYNCInfo() close connection failed:", err)
						}
						return err
					}

					// 获取RPC响应结果
					var reply RPCReply
					decoder := gob.NewDecoder(conn)
					err = decoder.Decode(reply)
					if err != nil {
						err = conn.Close()
						if err != nil {
							log.Errorln("SendSYNCInfo() close connection failed:", err)
						}
						return err
					}

					log.Debugln("got reply from rpc server:", reply)
				}
			}
		}
	}

	return nil
}
