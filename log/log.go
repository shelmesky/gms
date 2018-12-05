package disklog

import (
	"encoding/binary"
	"fmt"
	"github.com/shelmesky/gms/utils"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

const (
	MessageOffsetAndSizeField = 8
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

	return this.fileBuffer[pos : pos+length], nil
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

func FilenameToOffset(filename string) (int, error) {
	return strconv.Atoi(filename)
}

func OffsetToFilename(offset int) string {
	filename := strconv.Itoa(offset)
	filenameLength := len(filename)
	if filenameLength > 10 {
		filenameLength = 10
	}
	filename = strings.Repeat("0", 10-filenameLength) + filename
	return filename
}

// Log和Index文件
type LogIndexSegment struct {
	Log            FileSegment   // log文件
	Index          FileSegment   // index文件
	fileOpened     bool          // 是否已打开
	indexLoaded    bool          // 索引是否已载入
	indexList      []IndexRecord // index文件在内存中的数据结构
	startOffset    int           // 起始offset
	entrySize      int           // 当前index总的条目数量
	currentOffset  int           // 当前最大的offset，写入索引记录时用
	currentFilePos int           // 当前活动文件的写入位置，写入索引记录时用
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

	this.startOffset, err = FilenameToOffset(filename)
	if err != nil {
		return err
	}

	return nil
}

/*
从文件载入索引记录到内存数据
索引在文件中的记录格式为:
1, 21
2, 39
3, 83
N, ....

每条记录第一个数据为消息的offset，即消息序号
第二个字段为消息存储在Log文件中开始的位置

日志在文件中的格式为:
offset uint32
size   uint32
body   N bytes
*/
func (this *LogIndexSegment) LoadIndex() error {
	var indexList []IndexRecord
	var lastOffset int
	var lastMessagePos int
	var lastMessageSize uint32
	var logFileEndPos int
	var err error

	pos := 0

	for {
		var indexRecord IndexRecord

		// 读取4字节的offset
		offset, err := this.Index.ReadUInt32(pos)
		if err != nil {
			goto failed
		}
		pos += 4

		// 读取4字节的文件位置
		messagePos, err := this.Index.ReadUInt32(pos)
		if err != nil {
			goto failed
		}
		pos += 4

		// 如果读取到的offset和messagePos都为0
		// 说明读到了索引文件的末尾
		if offset == 0 && messagePos == 0 {
			// 设置index文件的最后写入位置
			// 当最后检测到两个字段都为0,要回退8个字节
			this.Index.dataWritten = pos - 8
			break
		}

		// 增加index的记录数量
		this.entrySize += 1

		lastOffset = int(offset)
		lastMessagePos = int(messagePos)

		// 将Index的记录插入IndexList，用于根据offset读取时的查找
		indexRecord.offset = int(offset)
		indexRecord.filePos = int(messagePos)
		indexList = append(indexList, indexRecord)
	}

	if len(indexList) > 0 {
		// 设置当前Index文件最后的offset和文件位置
		this.currentOffset = lastOffset

		// 读取最后一条消息的大小到lastMessageSize
		lastMessageSize, err = this.Log.ReadUInt32(lastMessagePos + 4)
		if err != nil {
			goto failed
		}

		// 在.log文件中最后一条消息的文件位置等于：
		// index记录中最后一条消息的开始位置 + log文件中获取到的最后一条消息的大小 + 消息头部的8字节(两个字段)
		logFileEndPos = lastMessagePos + int(lastMessageSize) + MessageOffsetAndSizeField

		// 设置.log文件最后的写入位置
		this.Log.dataWritten = logFileEndPos
		// 设置.index记录中将来要记录的消息在.log文件中的开始位置
		this.currentFilePos = logFileEndPos

		// 设置索引记录的列表
		this.indexList = indexList

		return nil
	} else {
		return utils.EmptyIndexFile
	}
failed:
	return utils.LoadIndexError
}

func (this *LogIndexSegment) Search(offset int) {
	binarySearch := func(begin, end int) (int, int) {
		return 0, 0
	}
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

	// 将offset写入到Log
	written, err = this.Log.AppendUInt32(uint32(this.currentOffset))
	if err != nil {
		return err
	}

	if written != 4 {
		return utils.WrittenNotEnoughError
	}

	// 将消息长度写入到Log
	written, err = this.Log.AppendUInt32(uint32(length))
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

	// 每插入一条数据就在内存的IndexList中追加索引记录
	indexRecord := IndexRecord{this.currentOffset, this.currentOffset}
	this.indexList = append(this.indexList, indexRecord)

	// 设置当前.log文件的偏移位置：
	// 写入数据的长度+头部的offset和size字段长度
	this.currentFilePos += length + MessageOffsetAndSizeField

	// 增加index的记录数量
	this.entrySize += 1

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
