package common

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	etcd "go.etcd.io/etcd/clientv3"
	"io"
	"net"
	"unsafe"
)

const (
	REQUEST_LEN         = 24   // Request结构的长度
	WRITE_MESSAGE_LEN   = 40   // 写入型消息的头部长度
	READ_BUF_SIZE       = 4096 // RingBuffer读取缓冲区大小
	WRITE_BUF_SIZE      = 4096 // RingBuffer写入缓冲区大小
	TOPIC_NAME_LEN      = 128  // topic名字允许的最大长度
	PARTITION_NUM_LEN   = 128  // partition分区号允许的最大长度
	RESPONSE_HEADER_LEN = 80
)

const (
	Write          = 1 // 写入类消息
	Read           = 2 // 读取类消息
	CreateTopic    = 3 // 创建topic
	StartSyncTopic = 4 // 开始同步topic
)

/************************************************************************/

// 每个消息都有的请求头部
type RequestHeader struct {
	TotalLength    uint64 // 消息的总长度
	Version        uint16 // 版本号
	Sequence       uint32 // 序列号
	MetaDataLength uint32 // 元数据
	BodyLength     uint32 // 消息主题长度
}

func BytesToRequest(data []byte, length int) *RequestHeader {
	var r *RequestHeader = *(**RequestHeader)(unsafe.Pointer(&data))
	return r
}

func RequestToBytes(request *RequestHeader) []byte {
	length := unsafe.Sizeof(*request)
	bytes := &Slice{
		addr: uintptr(unsafe.Pointer(request)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(bytes))
	return data
}

func NewRequest(metaData, bodyData []byte) (net.Buffers, uint64) {
	var request RequestHeader
	var netBuffer net.Buffers

	// 设置request的版本号和请求序号
	request.Version = 1001
	request.Sequence = 2

	metaDataLength := len(metaData)
	bodyLength := len(bodyData)

	// 设置request中的各种长度属性
	request.BodyLength = uint32(bodyLength)                                             // body数据长度
	request.MetaDataLength = uint32(metaDataLength)                                     // meta数据长度
	request.TotalLength = 8 + REQUEST_LEN + uint64(metaDataLength) + uint64(bodyLength) // 总长度

	// 将request结构体转换为[]byte
	requestBytes := RequestToBytes(&request)

	// 写入总长度到一个8字节的数组中
	totalLengthBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(totalLengthBytes, request.TotalLength)

	// 想net.Buffers中追加字节数组
	netBuffer = append(netBuffer, totalLengthBytes) // 数据包总长度数据
	netBuffer = append(netBuffer, requestBytes)     // request结构数据
	netBuffer = append(netBuffer, metaData)         // meta结构数据
	if bodyLength > 0 {
		netBuffer = append(netBuffer, bodyData) // body数据
	}

	// 返回net.Buffers和总长度
	return netBuffer, request.TotalLength
}

/************************************************************************/

type Response struct {
	buffer net.Buffers
	header ResponseHeader
}

// 每个响应结构都有的请求头部
type ResponseHeader struct {
	TotalLength uint64
	Code        uint32
	Message     [64]byte
	BodyLength  uint32
}

func NewResponse(code uint32, message string, body []byte) *Response {
	var responseHeader ResponseHeader
	var netBuffers net.Buffers
	var response Response

	responseHeader.TotalLength = 80
	responseHeader.Code = code

	n := 0
	if len(message) > 64 {
		n = 64
	} else {
		n = len(message)
	}
	for i := 0; i < n; i++ {
		responseHeader.Message[i] = message[i]
	}

	netBuffers = append(netBuffers, ResponseHeaderToBytes(&responseHeader))

	bodyLen := len(body)
	if bodyLen > 0 {
		netBuffers = append(netBuffers, body)
		responseHeader.TotalLength += uint64(bodyLen)
	}

	response.buffer = netBuffers
	response.header = responseHeader

	return &response
}

func (this *Response) WriteTo(conn net.Conn) error {
	// 使用writev系用调用发送数据
	n, err := this.buffer.WriteTo(conn)
	if uint64(n) != this.header.TotalLength || err != nil {
		logrus.Printf("Writev failed, total length: %d, data written: %d, error: %v\n",
			this.header.TotalLength, n, err)
		if err = conn.Close(); err != nil {
			logrus.Println("close socket failed:", err)
		}
	}

	return nil
}

func ResponseHeaderToBytes(request *ResponseHeader) []byte {
	length := unsafe.Sizeof(*request)
	bytes := &Slice{
		addr: uintptr(unsafe.Pointer(request)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(bytes))
	return data
}

func BytesToResponseHeader(data []byte) *ResponseHeader {
	var r *ResponseHeader = *(**ResponseHeader)(unsafe.Pointer(&data))
	return r
}

func ReadResponseHeader(conn *net.TCPConn) (*ResponseHeader, error) {
	responseBuffer := make([]byte, RESPONSE_HEADER_LEN)
	readN, err := io.ReadFull(conn, responseBuffer)
	if err != nil {
		logrus.Printf("read data from tcp conn failed, expect %d but read %d: %s\n",
			RESPONSE_HEADER_LEN, readN, err)
		return nil, err
	}

	return BytesToResponseHeader(responseBuffer), nil
}

/************************************************************************/
// 保存在meta中, 作为处理body的辅助信息
// 客户端消息写入命令
type WriteMessageAction struct {
	Action          uint32
	TopicName       [TOPIC_NAME_LEN]byte
	PartitionNumber [32]byte
}

func BytesToWriteMessageAction(data []byte) *WriteMessageAction {
	var act *WriteMessageAction = *(**WriteMessageAction)(unsafe.Pointer(&data))
	return act
}

func WriteMessageActionToBytes(action *WriteMessageAction) []byte {
	length := unsafe.Sizeof(*action)
	b := &Slice{
		addr: uintptr(unsafe.Pointer(action)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(b))
	return data
}

func NewWriteMessageAction(topicName, PartitionNumber string) []byte {
	var writeMessageAction WriteMessageAction
	writeMessageAction.Action = Write

	if len(topicName) > TOPIC_NAME_LEN {
		panic("topic name is too large")
	}
	for i := 0; i < len(topicName); i++ {
		writeMessageAction.TopicName[i] = topicName[i]
	}

	if len(PartitionNumber) > PARTITION_NUM_LEN {
		panic("partition mum is too large")
	}
	for i := 0; i < len(PartitionNumber); i++ {
		writeMessageAction.PartitionNumber[i] = PartitionNumber[i]
	}
	return WriteMessageActionToBytes(&writeMessageAction)
}

/************************************************************************/

// 客户端读取消息命令
type ReadMessageAction struct {
	Action          uint32
	TopicName       [TOPIC_NAME_LEN]byte
	PartitionNumber [32]byte
	TargetOffset    uint32
	Count           uint32
}

func BytesToReadMessageAction(data []byte) *ReadMessageAction {
	var act *ReadMessageAction = *(**ReadMessageAction)(unsafe.Pointer(&data))
	return act
}

func ReadMessageActionToBytes(action *ReadMessageAction) []byte {
	length := unsafe.Sizeof(*action)
	b := &Slice{
		addr: uintptr(unsafe.Pointer(action)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(b))
	return data
}

func NewReadMessageAction(topicName, PartitionNumber string, targetOffset, count uint32) []byte {
	var readMessageAction ReadMessageAction
	readMessageAction.Action = Read

	if len(topicName) > TOPIC_NAME_LEN {
		panic("topic name is too large")
	}
	for i := 0; i < len(topicName); i++ {
		readMessageAction.TopicName[i] = topicName[i]
	}

	if len(PartitionNumber) > PARTITION_NUM_LEN {
		panic("partition mum is too large")
	}
	for i := 0; i < len(PartitionNumber); i++ {
		readMessageAction.PartitionNumber[i] = PartitionNumber[i]
	}

	readMessageAction.TargetOffset = targetOffset
	readMessageAction.Count = count

	return ReadMessageActionToBytes(&readMessageAction)
}

/************************************************************************/
// 创建topic请求
type CreateTopicAction struct {
	Action         uint32
	TopicName      [TOPIC_NAME_LEN]byte
	PartitionCount uint32
	ReplicaCount   uint32
}

func BytesToCreateTopicAction(data []byte) *CreateTopicAction {
	var act *CreateTopicAction = *(**CreateTopicAction)(unsafe.Pointer(&data))
	return act
}

func CreateTopicActionToBytes(action *CreateTopicAction) []byte {
	length := unsafe.Sizeof(*action)
	b := &Slice{
		addr: uintptr(unsafe.Pointer(action)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(b))
	return data
}

func NewCreateTopicAction(topicName string, PartitionCount, ReplicaCount uint32) []byte {
	var action CreateTopicAction
	action.Action = CreateTopic

	if len(topicName) > TOPIC_NAME_LEN {
		panic("topic name is too large")
	}
	for i := 0; i < len(topicName); i++ {
		action.TopicName[i] = topicName[i]
	}

	action.PartitionCount = PartitionCount
	action.ReplicaCount = ReplicaCount

	return CreateTopicActionToBytes(&action)
}

/************************************************************************/

// 当Request.BodyLength 不等于 WriteMessageType.Length
// 说明消息是批量发送的

// 写入到磁盘的消息结构
type WriteMessageType struct {
	Length      uint64 // 消息长度
	CRC32       uint32 // CRC32
	Magic       uint32 // 魔法数字
	Attributes  uint32 // 属性
	KeyLength   uint64 // KEY的长度
	ValueLength uint64 // VALUE的长度
}

type Slice struct {
	addr uintptr
	len  int
	cap  int
}

func BytesToMessage(data []byte) *WriteMessageType {
	var m *WriteMessageType = *(**WriteMessageType)(unsafe.Pointer(&data))
	return m
}

func MessageToBytes(message *WriteMessageType) []byte {
	length := unsafe.Sizeof(*message)
	bytes := &Slice{
		addr: uintptr(unsafe.Pointer(message)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(bytes))
	return data
}

/************************************************************************/

// 保存在etcd中的Node信息
type Node struct {
	IPAddress string `json:"ip_address"`
	Port      int    `json:"port"`
	RPCPort   int    `json:"rpc_port"`
	NodeID    string `json:"node_id"`
	StartTime int64  `json:"start_time"`
}

func ETCDGetKey(key string, withPrefix bool) (*etcd.GetResponse, error) {
	var err error
	var client *etcd.Client
	var resp *etcd.GetResponse

	// 使用新的etcd连接
	client, err = etcd.New(etcd.Config{
		Endpoints: []string{GlobalConfig.EtcdServer},
	})

	if err != nil {
		err = errors.Wrap(err, "ETCDGetKeys() connect to etcd failed")
		return resp, err
	}

	kv := etcd.NewKV(client)

	if withPrefix {
		resp, err = kv.Get(context.Background(), key, etcd.WithPrefix())
	} else {
		resp, err = kv.Get(context.Background(), key)
	}

	if err != nil {
		err = fmt.Errorf("ETCDGetKeys() %s: get %s from etcd failed\n", err, key)
		return resp, err
	}

	return resp, nil
}

// 获取etcd中所有节点的列表
func GetAllNodes() ([]Node, error) {
	var nodeList []Node
	var err error

	key := "/brokers/ids/"
	getResp, err := ETCDGetKey(key, true)
	if err != nil {
		return nodeList, err
	}

	if len(getResp.Kvs) == 0 {
		return nodeList, fmt.Errorf("get empty node list from etcd")
	}

	for idx := range getResp.Kvs {
		var tempNode Node
		kv := getResp.Kvs[idx]
		valueBytes := kv.Value
		err = json.Unmarshal(valueBytes, &tempNode)
		if err != nil {
			return nodeList, err
		}
		nodeList = append(nodeList, tempNode)
	}

	return nodeList, nil
}

func GetSingleNode(nodeID string) (Node, error) {
	var tempNode Node
	var err error

	key := fmt.Sprintf("/brokers/ids/%s", nodeID)
	getResp, err := ETCDGetKey(key, true)

	if err != nil {
		return tempNode, err
	}

	if len(getResp.Kvs) == 0 {
		return tempNode, fmt.Errorf("can not find node [%s] in etcd\n", nodeID)
	}

	kv := getResp.Kvs[0]
	valueBytes := kv.Value
	err = json.Unmarshal(valueBytes, &tempNode)
	if err != nil {
		return tempNode, err
	}

	return tempNode, nil
}

/************************************************************************/

type StartSyncTopicAction struct {
	Action    uint32
	TopicName [TOPIC_NAME_LEN]byte
}

func BytesToStartSyncTopicAction(data []byte) *StartSyncTopicAction {
	var act *StartSyncTopicAction = *(**StartSyncTopicAction)(unsafe.Pointer(&data))
	return act
}

func StartSyncTopicActionToBytes(action *StartSyncTopicAction) []byte {
	length := unsafe.Sizeof(*action)
	b := &Slice{
		addr: uintptr(unsafe.Pointer(action)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(b))
	return data
}

func NewStartSyncTopicAction(topicName string) []byte {
	var action StartSyncTopicAction
	action.Action = StartSyncTopic

	if len(topicName) > TOPIC_NAME_LEN {
		panic("topic name is too large")
	}
	for i := 0; i < len(topicName); i++ {
		action.TopicName[i] = topicName[i]
	}

	return StartSyncTopicActionToBytes(&action)
}
