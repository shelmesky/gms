package client

import (
	"bytes"
	"github.com/shelmesky/gms/common"
)

func NewMessage() *common.Message {
	var message common.Message
	return &message
}

func (this *common.Message) Bytes() []byte {
	var buffer bytes.Buffer

	return buffer.Bytes()
}

func GetDemoMessage() []byte {
	message := NewMessage()

	return message.Bytes()
}
