package common

// 消息格式
type Message struct {
	crc32        int    // crc32校验
	magic        byte   // magic数字
	attributes   byte   // 属性组合
	keyLength    int    // key的长度
	keyPayload   []byte // key的内容
	valueLength  int    // value的长度
	valuePayload []byte // value的内容
}

// 生产者写请求
type WriteRequest struct {
	topic     string
	partition int
	message   Message
}

// 消费者读请求
type ReadRequest struct {
	topic       string
	partition   int
	startOffset int
	endOffset   int
	readSize    int
}
