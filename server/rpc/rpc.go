package rpc

import (
	"encoding/gob"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/partition"
	log "github.com/sirupsen/logrus"
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
