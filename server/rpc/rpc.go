package rpc

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

type InternalRPC int

func (*InternalRPC) HandleCreateTopic(args *NodePartitionReplicaInfo, reply *RPCReply) error {
	return nil
}

func SendCreateTopicJob(args *NodePartitionReplicaInfo) {

}
