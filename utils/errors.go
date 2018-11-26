package utils

import (
	"errors"
)

var (
	ZeroLengthError = errors.New("length is zero")
	TooLargeLengthError = errors.New("length is too larger")
	CopyNotEnoughError = errors.New("not copy enough bytes")
)