package utils

import (
	"errors"
)

var (
	ZeroLengthError          = errors.New("length is zero")
	TooLargeLengthError      = errors.New("length is too larger")
	CopyNotEnoughError       = errors.New("not copy enough bytes")
	WrittenNotEnoughError    = errors.New("data was not written enough")
	LogFileRemainSizeSmall   = errors.New("remain size of log file is too small.")
	IndexFileRemainSizeSmall = errors.New("remain size of index file is too small.")
	TargetNotFound           = errors.New("can not find target")
	TargetGreatThanCommitted = errors.New("target great than committed in segment")
	IndexIsIllegal           = errors.New("index of list is illegal")
	FileAlreadyExist         = errors.New("directory or file is already exists")
	ParameterTopicMissed     = errors.New("parameter topic name was missed")
	ServerError              = errors.New("server error was occurred")
)
