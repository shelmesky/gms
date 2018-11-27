package client

import "bytes"

type Message struct {
	crc32        int    // crc32校验
	magic        byte   // magic数字
	attributes   byte   // 属性组合
	keyLength    int    // key的长度
	keyPayload   []byte // key的内容
	valueLength  int    // value的长度
	valuePayload []byte // value的内容
}

func NewMessage() *Message{
	var message Message
	return &message
}

func (this *Message) Bytes() []byte {
	var buffer bytes.Buffer

	return buffer.Bytes()
}


func GetDemoMessage() []byte {
	message := NewMessage()

	return message.Bytes()
}