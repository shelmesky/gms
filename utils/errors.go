package utils

import (
	"errors"
)

var (
	ZeroLengthError       = errors.New("length is zero")
	TooLargeLengthError   = errors.New("length is too larger")
	CopyNotEnoughError    = errors.New("not copy enough bytes")
	WrittenNotEnoughError = errors.New("data was not written enough")
	LoadIndexError        = errors.New("load index from disk failed")
	EmptyIndexFile        = errors.New("can not load empty index file")
)
