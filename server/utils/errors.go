package utils

import (
	"errors"
)

var (
	ZeroLengthError               = errors.New("length is zero")
	TooLargeLengthError           = errors.New("length is too larger")
	CopyNotEnoughError            = errors.New("not copy enough bytes")
	WrittenNotEnoughError         = errors.New("data was not written enough")
	LogFileRemainSizeSmall        = errors.New("remain size of log file is too small.")
	IndexFileRemainSizeSmall      = errors.New("remain size of index file is too small.")
	TargetNotFound                = errors.New("can not find target")
	TargetGreatThanCommitted      = errors.New("target great than committed in segment")
	IndexIsIllegal                = errors.New("index of list is illegal")
	FileAlreadyExist              = errors.New("directory or file is already exists")
	ParameterTopicMissed          = errors.New("parameter topic name was missed")
	ParameterPartitionIndexMissed = errors.New("parameter partition index was missed")
	ServerError                   = errors.New("ReadMessage can not find topic")
	PartitionNotExist             = errors.New("partition num does not exist")
	MessageLengthInvalid          = errors.New("length of message bytes is invalid")
	CloseConnError                = errors.New("close client connection failed")
	OffsetBigError                = errors.New("offset in request is big than partition's offset")
	PartitionNoISRList            = errors.New("can not find any follower replicate")
	NotEnoughReplicas             = errors.New("there's no enough replicas for partition")
	ISRFollowerTimeout            = errors.New("All of the ISR follower list was not active")
	UnSupportAckMode              = errors.New("unsupported ack mode")
)
