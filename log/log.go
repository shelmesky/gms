package disklog

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"syscall"
)

type FileSegment struct {
	filename    string       // 文件名
	size        int          // 文件大小
	file        *os.File     // 文件对象
	fileBuffer  []byte       // mmap映射的内存区域
	dataWritten int          // 已经写入的数据大小
	lock        sync.RWMutex // 写入锁
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
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE)

	if err != nil {
		return logSegment, err
	}

	return logSegment, nil
}

func CreateLogSegment(filename string, capacity int) error {
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

func OpenRDOnlyLogSegment(filename string, capacity int) (FileSegment, error) {
	return NewLogSegment(filename, os.O_RDONLY, capacity)
}

func OpenReadWriteLogSegment(filename string, capacity int) (FileSegment, error) {
	return NewLogSegment(filename, os.O_RDWR, capacity)
}

func (this *FileSegment) Close() {
	this.file.Close()
}

func (this *FileSegment) AppendBytes(data []byte, length int) (int, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("data length is zero")
	}

	this.lock.Lock()
	defer this.lock.Unlock()

	written_len := copy(this.fileBuffer, data[:length])
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
