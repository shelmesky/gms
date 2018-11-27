package disklog

import (
	"encoding/binary"
	"fmt"
	"github.com/shelmesky/gms/utils"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"syscall"
	"unsafe"
)

func CreateFile(filename string, capacity int) error {
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

type FileSegment struct {
	filename      string       // 文件名
	size          int          // 文件大小
	file          *os.File     // 文件对象
	fileBuffer    []byte       // mmap映射的内存区域
	dataWritten   int          // 已经写入的数据大小
	dataCommitted int          // 已经提交到持久存储的大小
	lock          sync.RWMutex // 写入锁
}

func OpenFileSegment(filename string, flag int, capacity int) (FileSegment, error) {
	var logSegment FileSegment
	var prot int

	logSegment.filename = filename

	file, err := os.OpenFile(logSegment.filename, flag, 0664)
	if err != nil {
		return logSegment, err
	}

	logSegment.file = file
	logSegment.size = capacity

	if flag == 0x00 {
		prot = syscall.PROT_READ
	} else {
		prot = syscall.PROT_READ | syscall.PROT_WRITE
	}

	logSegment.fileBuffer, err = syscall.Mmap(int(file.Fd()), 0, int(capacity), prot, syscall.MAP_SHARED)

	if err != nil {
		return logSegment, err
	}

	err = syscall.Madvise(logSegment.fileBuffer, syscall.MADV_SEQUENTIAL)

	if err != nil {
		return logSegment, err
	}

	return logSegment, nil
}

func OpenRDOnlyLogSegment(filename string, capacity int) (FileSegment, error) {
	return OpenFileSegment(filename, os.O_RDONLY, capacity)
}

func OpenReadWriteLogSegment(filename string, capacity int) (FileSegment, error) {
	return OpenFileSegment(filename, os.O_RDWR, capacity)
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
	if len(data) <= 0 {
		return 0, utils.ZeroLengthError
	}

	if len(data) > (this.Capacity() - this.Used()) {
		return 0, utils.TooLargeLengthError
	}

	this.lock.Lock()
	defer this.lock.Unlock()

	written_len := copy(this.fileBuffer[this.dataWritten:], data[:length])
	if written_len != length {
		return 0, utils.CopyNotEnoughError
	}
	this.dataWritten += length

	return written_len, nil
}

func (this *FileSegment) AppendUInt32(data uint32) (int, error) {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, data)
	return this.AppendBytes(bs, 4)
}

func (this *FileSegment) AppendUInt64(data uint64) (int, error) {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, data)
	return this.AppendBytes(bs, 8)
}

func (this *FileSegment) ReadBytes(pos int, length int) ([]byte, error) {
	var result []byte

	if length <= 0 {
		return result, utils.ZeroLengthError
	}

	if length > this.Capacity() {
		return result, utils.TooLargeLengthError
	}

	result = make([]byte, length)

	dataCopied := copy(result, this.fileBuffer[pos:pos+length])
	if dataCopied != length {
		return result, utils.CopyNotEnoughError
	}

	return result, nil
}

func (this *FileSegment) ReadUInt32(pos int) (uint32, error) {
	bytesRead, err := this.ReadBytes(pos, 4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(bytesRead), nil
}

func (this *FileSegment) ReadUInt64(pos int) (uint64, error) {
	bytesRead, err := this.ReadBytes(pos, 8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(bytesRead), nil
}

func (this *FileSegment) SendBytesToSock(sockFD int, offset *int64, length int) {
	syscall.Sendfile(sockFD, int(this.file.Fd()), offset, length)
}

func (this *FileSegment) Capacity() int {
	return this.size
}

func (this *FileSegment) Used() int {
	return this.dataWritten
}

// 内存中Index的一条记录
type IndexRecord struct {
	offset  int
	filePos int
}

// Log和Index文件
type LogIndexSegment struct {
	Log            FileSegment   // log文件
	Index          FileSegment   // index文件
	fileOpened         bool          // 是否已打开
	indexLoaded         bool          // 索引是否已载入
	indexList      []IndexRecord // index文件在内存中的数据结构
	startOffset    int           // 起始offset
	currentOffset  int           // 当前最大的offset
	currentFilePos int           // 当前活动文件的写入位置
	lock           sync.RWMutex  // 读写锁
}

func CreateLogIndexSegmentFile(filename string, logCapacity, indexCapacity int) error {
	err := CreateFile(filename+".log", logCapacity)
	if err != nil {
		return err
	}

	err = CreateFile(filename+".index", indexCapacity)
	if err != nil {
		return err
	}

	return nil
}

func (this *LogIndexSegment) Open(filename string, writable bool, logCapacity, indexCapacity int) error {
	var err error

	this.lock.Lock()
	defer this.lock.Unlock()

	if writable {
		this.Log, err = OpenReadWriteLogSegment(filename+".log", logCapacity)
		if err != nil {
			return err
		}

		this.Index, err = OpenReadWriteLogSegment(filename+".index", indexCapacity)
		if err != nil {
			return err
		}

		this.fileOpened = true

	} else {
		this.Log, err = OpenRDOnlyLogSegment(filename+".log", logCapacity)
		if err != nil {
			return err
		}

		this.Index, err = OpenRDOnlyLogSegment(filename+".index", indexCapacity)
		if err != nil {
			return err
		}

		this.fileOpened = true
	}

	return nil
}

func (this *LogIndexSegment) LoadIndex() error {
	var indexList []IndexRecord
	var err error
	var offset uint32
	var messagePos uint32

	pos := 0

	for {
		var indexRecord IndexRecord

		offset, err = this.Index.ReadUInt32(pos)
		if err != nil {
			goto failed
		}
		pos += 4

		messagePos, err = this.Index.ReadUInt32(pos)
		if err != nil {
			goto failed
		}
		pos += 4

		if offset == 0 && messagePos == 0 {
			break
		}

		indexRecord.offset = int(offset)
		indexRecord.filePos = int(messagePos)

		indexList = append(indexList, indexRecord)
	}

	this.currentOffset = int(offset)
	this.currentFilePos = int(messagePos)

	this.indexLoaded = true

	return nil
failed:
	return utils.LoadIndexError
}

func (this *LogIndexSegment) Search(offset int) {

}

func (this *LogIndexSegment) AppendBytes(data []byte, length int) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	// 在index中写入offset
	this.currentOffset += 1
	written, err := this.Index.AppendUInt32(uint32(this.currentOffset))
	if err != nil {
		return err
	}

	if written != 4 {
		return utils.WrittenNotEnoughError
	}

	// 将数据写入到Log
	written, err = this.Log.AppendBytes(data, length)
	if err != nil {
		return err
	}

	if written != length {
		return utils.WrittenNotEnoughError
	}

	// 在Index中写入消息的文件位置
	written, err = this.Index.AppendUInt32(uint32(this.currentFilePos))
	if err != nil {
		return err
	}

	if written != 4 {
		return utils.WrittenNotEnoughError
	}

	indexRecord := IndexRecord{this.currentOffset, this.currentOffset}
	this.indexList = append(this.indexList, indexRecord)

	this.currentFilePos += length

	return nil
}

// 管理partition对应目录下的所有文件
type DiskLog struct {
	dirName       string            // 目录名称
	segments      []LogIndexSegment // 按照文件名排序的Segment
	activeSegment LogIndexSegment   // 当前活动的Segment
}

/*
当前ActiveSegment文件大小不足时
创建新的Segment
1. 查找ActiveSegment的最大offset
2. 以offset问文件名创建Segment
3. 关闭ActiveSegment
4. 把新的Segment作为ActiveSegment
*/
func (log *DiskLog) CreateSegment(filename string, logCapacity, indexCapacity int) LogIndexSegment {
	var logIndexSeg LogIndexSegment

	return logIndexSeg
}

/*
读取partition目录中的所有log和index文件
按照文件名中包含的offset排序
找到包含最大offset的segment当作ActiveSegment
并在内存中维护一份index的数据拷贝map[int]int
以此增加读取记录时index的访问速度
*/
func (log *DiskLog) Init(dirName string) error {
	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		return err
	}

	for idx := range files {
		file := files[idx]
		file.Name()
	}

	return nil
}

/*
在partition级别写入字节数据到存储
1. 检查ActiveSegment是否正常
2. 查找内存index中找到最大的offset
3. 增加offset并将新的offset写入到index文件
4. 找到上一条写入后的文件物理位置，作为此次新数据的起始位置
5. 将此次数据在文件的起始位置写入index
6. 在内存index数据结构中插入新写入的index信息

index中的每一行都是一对offset和filePos:
0,156
10,300
20,742

index是稀疏索引，文件尺寸相对较小，但会增加查找时间
*/
func (log *DiskLog) WriteBytes(data []byte, length int) (int, error) {
	return 0, nil
}

func (log *DiskLog) SendBytesToSock(offset, length, sockFD int) error {
	return nil
}
