package disklog

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"syscall"
	"unsafe"
)

type FileSegment struct {
	filename      string       // 文件名
	size          int          // 文件大小
	file          *os.File     // 文件对象
	fileBuffer    []byte       // mmap映射的内存区域
	dataWritten   int          // 已经写入的数据大小
	dataCommitted int          // 已经提交到持久存储的大小
	lock          sync.RWMutex // 写入锁
}

func NewLogSegment(filename string, flag int, capacity int) (FileSegment, error) {
	var logSegment FileSegment

	logSegment.filename = filename

	file, err := os.OpenFile(logSegment.filename, flag, 0664)
	if err != nil {
		return logSegment, err
	}

	logSegment.file = file
	logSegment.size = capacity

	logSegment.fileBuffer, err = syscall.Mmap(int(file.Fd()), 0, int(capacity),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		return logSegment, err
	}

	err = syscall.Madvise(logSegment.fileBuffer, syscall.MADV_SEQUENTIAL)

	if err != nil {
		return logSegment, err
	}

	return logSegment, nil
}

func CreateLogSegment(filename string, capacity int) error {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
		if err != nil {
			return err
		}

		err = syscall.Fallocate(int(file.Fd()), 0, 0, int64(capacity))
		if err != nil {
			return err
		}

		file.Close()

		return nil
	}

	return os.ErrExist
}

func OpenRDOnlyLogSegment(filename string, capacity int) (FileSegment, error) {
	return NewLogSegment(filename, os.O_RDONLY, capacity)
}

func OpenReadWriteLogSegment(filename string, capacity int) (FileSegment, error) {
	return NewLogSegment(filename, os.O_RDWR, capacity)
}

func (this *FileSegment) BufferHeader() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(&this.fileBuffer))
}

func (this *FileSegment) Force() error {
	this.lock.Lock()
	defer this.lock.Unlock()

	header := this.BufferHeader()

	syncAddr := header.Data + uintptr(this.dataCommitted)
	syncLength := uintptr(this.dataWritten - this.dataCommitted)
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, syncAddr, syncLength, syscall.MS_SYNC)
	if err != 0 {
		return fmt.Errorf(err.Error())
	}

	this.dataCommitted = this.dataWritten

	return nil
}

func (this *FileSegment) Close() error {
	err := syscall.Munmap(this.fileBuffer)
	if err != nil {
		return err
	}
	return this.file.Close()
}

func (this *FileSegment) AppendBytes(data []byte, length int) (int, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("data length is zero")
	}

	this.lock.Lock()
	defer this.lock.Unlock()

	written_len := copy(this.fileBuffer[this.dataWritten:], data[:length])
	if written_len != length {
		return 0, fmt.Errorf("data length copied is not equal to source length")
	}
	this.dataWritten += length

	return written_len, nil
}

func (this *FileSegment) AppendInt32(data int32) {

}

func (this *FileSegment) AppendInt64(data int64) {

}

func (this *FileSegment) ReadBytes(target []byte, offset int, length int) {

}

func (this *FileSegment) ReadInt32(offset int) int32 {
	return 0
}

func (this *FileSegment) ReadInt64(offset int) int64 {
	return 0
}

func (this *FileSegment) Capacity() int {
	return this.size
}

func (this *FileSegment) Used() int {
	return this.dataWritten
}

type DiskLog struct {
	dirname       string
	segments      []FileSegment
	activeSegment FileSegment
}

func (log *DiskLog) Init(dirname string) error {
	files, err := ioutil.ReadDir(dirname)
	if err != nil {
		return err
	}

	for idx := range files {
		file := files[idx]
		file.Name()
	}

	return nil
}
