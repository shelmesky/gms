package disklog

import (
	"encoding/binary"
	"fmt"
	"github.com/shelmesky/gms/server/utils"
	"io/ioutil"
	"math"
	"os"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

const (
	MessageOffsetAndSizeField = 8               // 存储在log文件中每个消息之前的两个字段大小
	IndexEntrySize            = 8               // index文件中每个数据项的大小
	IndexFileSize             = 1024 * 1024 * 1 // index文件大小
	LogFileSize               = 1024 * 1024 * 2 // log文件大小
	EntriesPerFile            = 10              // 每个log文件中保存多少条记录
	FileNameLength            = 19              // log和index文件名的长度
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

		err = file.Close()
		if err != nil {
			return err
		}

		return nil
	}

	return os.ErrExist
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
	if filenameLength > FileNameLength {
		filenameLength = FileNameLength
	}
	filename = strings.Repeat("0", FileNameLength-filenameLength) + filename
	return filename
}

/**********************************************************/

// 单个文件管理， 例如log或者index文件
type FileSegment struct {
	filename      string       // 文件名
	size          int          // 文件大小
	File          *os.File     // 文件对象
	fileBuffer    []byte       // mmap映射的内存区域
	dataWritten   int          // 已经写入的数据大小
	dataCommitted int          // 已经提交到持久存储的大小
	lock          sync.RWMutex // 写入锁
}

// 使用mmap方式打开文件
func OpenFileSegment(filename string, flag int, capacity int, populate bool) (FileSegment, error) {
	var logSegment FileSegment
	var prot int

	logSegment.filename = filename

	file, err := os.OpenFile(logSegment.filename, flag, 0664)
	if err != nil {
		return logSegment, err
	}

	logSegment.File = file
	logSegment.size = capacity

	if flag == 0x00 {
		prot = syscall.PROT_READ
	} else {
		prot = syscall.PROT_READ | syscall.PROT_WRITE
	}

	flags := syscall.MAP_FILE | syscall.MAP_SHARED
	if populate == true {
		flags |= syscall.MAP_POPULATE
	}

	logSegment.fileBuffer, err = syscall.Mmap(int(file.Fd()), 0, int(capacity), prot, flags)

	if err != nil {
		return logSegment, err
	}

	err = syscall.Madvise(logSegment.fileBuffer, syscall.MADV_SEQUENTIAL|syscall.MADV_WILLNEED)

	if err != nil {
		return logSegment, err
	}

	return logSegment, nil
}

func OpenRDOnlyLogSegment(filename string, capacity int, populate bool) (FileSegment, error) {
	return OpenFileSegment(filename, os.O_RDONLY, capacity, populate)
}

func OpenReadWriteLogSegment(filename string, capacity int, populate bool) (FileSegment, error) {
	return OpenFileSegment(filename, os.O_RDWR, capacity, populate)
}

func (this *FileSegment) BufferHeader() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(&this.fileBuffer))
}

// 调用syscall.SYS_MSYNC， 强制将未提交的部分提交到磁盘
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

// 关闭mmap方式打开的文件
func (this *FileSegment) Close() error {
	err := syscall.Munmap(this.fileBuffer)
	if err != nil {
		return err
	}
	return this.File.Close()
}

// 向单个文件中追加字节数据
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

// 向文件中追加一个uint32
func (this *FileSegment) AppendUInt32(data uint32) (int, error) {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, data)
	return this.AppendBytes(bs, 4)
}

// 向文件中追加一个uint64
func (this *FileSegment) AppendUInt64(data uint64) (int, error) {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, data)
	return this.AppendBytes(bs, 8)
}

// 从文件的指定位置读取指定长度的数据
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

// 从文件的指定位置读取一个uint32数字
func (this *FileSegment) ReadUInt32(pos int) (uint32, error) {
	bytesRead, err := this.ReadBytes(pos, 4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(bytesRead), nil
}

// 从文件的指定位置读取一个uint64数字
func (this *FileSegment) ReadUInt64(pos int) (uint64, error) {
	bytesRead, err := this.ReadBytes(pos, 8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(bytesRead), nil
}

// 返回当前文件的预设容量
func (this *FileSegment) Capacity() int {
	return this.size
}

// 返回目前文件已经写到哪个位置
func (this *FileSegment) Used() int {
	return this.dataWritten
}

// 返回文件目前剩余的容量
func (this *FileSegment) Remain() int {
	return this.size - this.dataWritten
}

/**********************************************************/

// 管理一对Log和Index文件
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
	warmEntries    int           // 在内核page cache中的索引数量
	lock           sync.RWMutex  // 读写锁
}

// 创建一对log和index文件
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

// 打开一对log和index文件
func (this *LogIndexSegment) Open(filename string, writable bool, logCapacity,
	indexCapacity int, populate bool) error {
	var err error

	this.lock.Lock()
	defer this.lock.Unlock()

	if writable {
		this.Log, err = OpenReadWriteLogSegment(filename+".log", logCapacity, populate)
		if err != nil {
			return err
		}

		this.Index, err = OpenReadWriteLogSegment(filename+".index", indexCapacity, populate)
		if err != nil {
			return err
		}

		this.fileOpened = true

	} else {
		this.Log, err = OpenRDOnlyLogSegment(filename+".log", logCapacity, populate)
		if err != nil {
			return err
		}

		this.Index, err = OpenRDOnlyLogSegment(filename+".index", indexCapacity, populate)
		if err != nil {
			return err
		}

		this.fileOpened = true
	}

	// 根据文件名获取初始的offset
	this.startOffset, err = FilenameToOffset(path.Base(path.Clean(filename)))
	if err != nil {
		return err
	}

	// 设置warm section的范围
	this.warmEntries = 8192

	return nil
}

// 关闭log和index文件
func (this *LogIndexSegment) Close() error {
	this.lock.RLock()
	defer this.lock.RUnlock()

	// 关闭之前刷新page cache数据到磁盘
	err := this.Index.Force()
	if err != nil {
		return err
	}

	err = this.Log.Force()
	if err != nil {
		return err
	}

	err = this.Index.Close()
	if err != nil {
		return err
	}

	err = this.Log.Close()
	if err != nil {
		return err
	}

	return nil
}

// 从log文件的第4个字节开始读取， 读一个uint32数， 即第一条log记录的size
// 如果为0说明此log文件没有保存任何记录
func (this *LogIndexSegment) ReallyNoLog() bool {
	firstLogSize, err := this.Log.ReadUInt32(4)
	if err != nil {
		panic("read uint32 from log failed:" + this.Log.filename)
	}
	if firstLogSize == 0 {
		return true
	}

	return false
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
	var lastOffset int
	var lastMessagePos int
	var lastMessageSize uint32
	var logFileEndPos int
	var err error

	pos := 0

	for {
		// 读取4字节的offset
		offset, err := this.Index.ReadUInt32(pos)
		if err != nil {
			return err
		}
		pos += 4

		// 读取4字节的文件位置
		messagePos, err := this.Index.ReadUInt32(pos)
		if err != nil {
			return err
		}
		pos += 4

		// 下面的if代码块是检查是否读到了index文件的末尾， 以及如何修正
		// 确保已经读取不止1条记录, 排除文件开始都为0的情况
		if this.entrySize > 0 {
			// 如果读取到的offset和messagePos都为0
			// 说明读到了索引文件的末尾
			if offset == 0 && messagePos == 0 {
				// 设置index文件的最后写入位置
				// 当最后检测到两个字段都为0

				// 如已经读取了1条记录, 说明加上本次是连续两次记录为0, 要回退16个字节
				// 如已经读取了大于1条即最少2条, 说明前一次读取是正常的, 则只要回退8字节
				if this.entrySize == 1 {
					// 当index文件只有一条记录且offset和messagePos都为0的情况下
					// 并不一定说明log文件没有记录
					// 这里就需要读log文件的开始来确认
					if this.ReallyNoLog() {
						this.Index.dataWritten = pos - 16
						this.entrySize -= 1
					} else {
						this.Index.dataWritten = pos - 8
					}
				} else if this.entrySize > 1 {
					this.Index.dataWritten = pos - 8
				}
				break
			}
		}

		// 增加index的记录数量
		this.entrySize += 1

		lastOffset = int(offset)
		lastMessagePos = int(messagePos)
	}

	if this.entrySize > 0 {
		// 设置当前Index文件最后的offset和文件位置

		this.currentOffset = lastOffset + 1

		// 当index中的offset和message pos都为0, 但log文件中确实有消息的情况下
		// 设置当前offset为1
		if lastOffset == 0 && lastMessagePos == 0 && this.entrySize == 1 {
			this.currentOffset = 1
		}

		// 读取最后一条消息的大小到lastMessageSize
		lastMessageSize, err = this.Log.ReadUInt32(lastMessagePos + 4)
		if err != nil {
			return err
		}

		// 在.log文件中最后一条消息的文件位置等于：
		// index记录中最后一条消息的开始位置 + log文件中获取到的最后一条消息的大小 + 消息头部的8字节(两个字段)
		logFileEndPos = lastMessagePos + int(lastMessageSize) + MessageOffsetAndSizeField

		// 设置.log文件最后的写入位置
		this.Log.dataWritten = logFileEndPos
		// 设置.index记录中将来要记录的消息在.log文件中的开始位置
		this.currentFilePos = logFileEndPos
	}
	return nil
}

// 根据offset返回Index索引条目的offset值
func (this *LogIndexSegment) GetIndexEntryKey(offset int) int {
	ret, err := this.Index.ReadUInt32(offset * IndexEntrySize)
	if err != nil {
		panic("read uint32 from index file failed")
	}
	return int(ret)
}

func (this *LogIndexSegment) GetIndexEntryValue(offset int) int {
	ret, err := this.Index.ReadUInt32(offset*IndexEntrySize + 4)
	if err != nil {
		panic("read uint32 from index file failed")
	}
	return int(ret)
}

func compareIndexEntry(found, offset int) int {
	if found > offset {
		return 1
	}
	if found < offset {
		return -1
	}

	return 0
}

// 搜索target在index文件中处于第几个记录
func (this *LogIndexSegment) SearchIndex(target int) (int, int) {
	binarySearch := func(begin, end int) (int, int) {
		var lo = begin
		var hi = end
		for {
			if lo >= hi {
				break
			}

			mid := int(math.Ceil(float64(hi)/2.0 + float64(lo)/2.0))
			found := this.GetIndexEntryKey(mid)
			compareResult := compareIndexEntry(found, target)
			if compareResult > 0 {
				hi = mid - 1
			} else if compareResult < 0 {
				lo = mid
			} else {
				return mid, mid
			}
		}

		var upperBound int
		if lo == this.entrySize-1 {
			upperBound = -1
		} else {
			upperBound = lo + 1
		}

		return lo, upperBound
	}

	/*
		firstHotEntry的结果有两种情况，一种为0,另一种为非0。
		为0时，说明index的数量小于8192，此时从文件的开始处搜索，直到文件结束。
		不为0时，又分两种情况，第一种是小于offset，此时搜索的范围在warm section内，
		即从firstHotEntry开始到文件结束，这样缩小了二分查找的范围
		第二种是大于offset，说明offset不在warm section范围内，
		则从文件开始处搜索，直到firstHotEntry的位置。
	*/

	firstHotEntry := int(math.Max(0, float64(this.entrySize-1-this.warmEntries)))
	if compareIndexEntry(this.GetIndexEntryKey(firstHotEntry), target) < 0 {
		return binarySearch(firstHotEntry, this.entrySize-1)
	}

	if compareIndexEntry(this.GetIndexEntryKey(0), target) > 0 {
		return -1, 0
	}

	return binarySearch(0, firstHotEntry)
}

// 根据target搜索在index文件中记录的消息在log文件中的偏移
func (this *LogIndexSegment) Search(target int) int {
	indexOffsetLogPos, _ := this.SearchIndex(target)
	if indexOffsetLogPos == -1 {
		return -1
	}

	return this.GetIndexEntryValue(indexOffsetLogPos)
}

func (this *LogIndexSegment) AppendBytes(data []byte, length int) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	// log文件容量不足
	if length > this.Log.Remain() {
		return utils.LogFileRemainSizeSmall
	}

	// index文件容量不足
	if this.Index.Remain() < 4 {
		return utils.IndexFileRemainSizeSmall
	}

	// segment文件中消息数量超过预设
	if (this.entrySize + 1) > EntriesPerFile {
		return utils.LogFileRemainSizeSmall
	}

	// 发现currentOffset(值为0)小于文件初始化的offset
	// 说明这是第一次写本文件，则应该加上初始化offset
	if this.currentOffset < this.startOffset {
		this.currentOffset += this.startOffset
	}

	// 在index中写入offset
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

	// 增加当前最大offset
	this.currentOffset += 1

	return nil
}

func (this *LogIndexSegment) GetCurrentOffset() int {
	return this.currentOffset
}

/**********************************************************/

type baseFileInfo struct {
	baseFileName  string
	IndexFileSize int64
	LogFileSize   int64
}

type sortedFileList []*baseFileInfo

func (list sortedFileList) Get(i int) *baseFileInfo {
	return list[i]
}

func (list sortedFileList) Set(value *baseFileInfo, i int) {
	if i > list.Len() {
		panic("")
	}
	list[i] = value
}

func (list sortedFileList) Len() int {
	return len(list)
}

func (list sortedFileList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func (list sortedFileList) Less(i, j int) bool {
	fileNameOffset_I, err := FilenameToOffset(list[i].baseFileName)
	if err != nil {
		panic(err.Error())
	}

	fileNameOffset_J, err := FilenameToOffset(list[j].baseFileName)
	if err != nil {
		panic(err.Error())
	}

	return fileNameOffset_I < fileNameOffset_J
}

// 管理partition对应目录下的所有文件
type DiskLog struct {
	dirName       string            // 目录名称
	segments      []LogIndexSegment // 按照文件名排序的Segment
	activeSegment LogIndexSegment   // 当前活动的Segment
	activeFile    *baseFileInfo     // 当前活动的文件
}

func (log *DiskLog) getFullPath(filename string) string {
	return path.Join(log.dirName, filename)
}

func (log *DiskLog) GetSegment(pos int) (*LogIndexSegment, error) {
	if pos < 0 || pos > len(log.segments)-1 {
		return nil, utils.IndexIsIllegal
	}

	return &log.segments[pos], nil
}

func (log *DiskLog) GetActiveSegment() *LogIndexSegment {
	return &log.activeSegment
}

func (log *DiskLog) SegmentLength() int {
	return len(log.segments)
}

func (log *DiskLog) Init(dirName string) error {
	log.dirName = dirName

	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		return err
	}

	// 如果当前目录没有segment, 则初始化一个新的
	// 接着重新读取目录
	if len(files) == 0 {
		// 创建新的log和index文件
		newFileBaseName := OffsetToFilename(0)
		newIndexFileSize := IndexFileSize
		newLogFileSize := LogFileSize
		err := CreateLogIndexSegmentFile(log.getFullPath(newFileBaseName), newLogFileSize, newIndexFileSize)
		if err != nil {
			return err
		}

		files, err = ioutil.ReadDir(dirName)
		if err != nil {
			return err
		}
	}

	fileMap := make(map[string]*baseFileInfo, len(files))

	// 循环读取文件并提取文件名和大小
	for idx := range files {
		fileName := files[idx].Name()
		fileSize := files[idx].Size()

		splitFile := strings.Split(path.Clean(path.Base(fileName)), ".")
		if len(splitFile) != 2 {
			return fmt.Errorf("split file failed:", fileName)
		}

		if _, ok := fileMap[splitFile[0]]; !ok {
			fileMap[splitFile[0]] = new(baseFileInfo)
		}

		baseFile := fileMap[splitFile[0]]
		baseFile.baseFileName = splitFile[0]

		if strings.Contains(fileName, ".index") {
			baseFile.IndexFileSize = fileSize
		}

		if strings.Contains(fileName, ".log") {
			baseFile.LogFileSize = fileSize
		}
	}

	var fileList sortedFileList
	for _, v := range fileMap {
		fileList = append(fileList, v)
	}

	sort.Sort(fileList)

	// 打开只读的segment(只读的segments中包含active segment的一个只读取副本)
	for i := 0; i < fileList.Len(); i++ {
		file := fileList.Get(i)
		fileBaseName := file.baseFileName
		indexFileSize := file.IndexFileSize
		logFileSize := file.LogFileSize

		var logIndexSegment LogIndexSegment
		err := logIndexSegment.Open(log.getFullPath(fileBaseName),
			false, int(logFileSize), int(indexFileSize), false)
		if err != nil {
			return fmt.Errorf("open log and index as read only file failed: %s\n", err)
		}

		err = logIndexSegment.LoadIndex()
		if err != nil {
			return err
		}

		log.segments = append(log.segments, logIndexSegment)
	}

	// 打开读写的segment, 作为active segment.
	// populate为true, 让内核预先分配好页表(prefault), 并预读文件(readahead).
	activeFile := fileList[fileList.Len()-1]
	fileBaseName := activeFile.baseFileName
	indexFileSize := activeFile.IndexFileSize
	logFileSize := activeFile.LogFileSize
	var activeSegment LogIndexSegment
	err = activeSegment.Open(log.getFullPath(fileBaseName),
		true, int(logFileSize), int(indexFileSize), true)
	if err != nil {
		return fmt.Errorf("open log and index as writable file failed: %s\n", err)
	}

	err = activeSegment.LoadIndex()
	if err != nil {
		return err
	}

	log.activeSegment = activeSegment
	log.activeFile = activeFile

	return nil
}

// 关闭当前active segment并创建新的
func (log *DiskLog) RegenerateActiveSegment() error {
	// 关闭当前活动的segment
	err := log.activeSegment.Close()
	if err != nil {
		return err
	}

	oldActiveFileToOffset, err := FilenameToOffset(log.activeFile.baseFileName)
	if err != nil {
		return err
	}
	// 新的segment的名字等于旧的segment的文件名数字加保存的记录数
	oldActiveFileToOffset += log.activeSegment.entrySize
	newSegmentOffset := oldActiveFileToOffset

	// 重新打开之前关闭的segment作为只读模式
	var readOnlySegment LogIndexSegment
	fileBaseName := log.activeFile.baseFileName
	indexFileSize := log.activeFile.IndexFileSize
	logFileSize := log.activeFile.LogFileSize

	err = readOnlySegment.Open(log.getFullPath(fileBaseName),
		false, int(logFileSize), int(indexFileSize), false)
	if err != nil {
		return fmt.Errorf("reopen active segment [%s] as read only failed: %s\n",
			fileBaseName, err.Error())
	}

	// 将只读模式的segment加入到segment列表
	log.segments = append(log.segments, readOnlySegment)

	// 创建新的log和index文件
	var newActiveSegment LogIndexSegment
	newFileBaseName := OffsetToFilename(newSegmentOffset)
	newIndexFileSize := IndexFileSize
	newLogFileSize := LogFileSize

	err = CreateLogIndexSegmentFile(log.getFullPath(newFileBaseName), newLogFileSize, newIndexFileSize)
	if err != nil {
		return err
	}

	err = newActiveSegment.Open(log.getFullPath(newFileBaseName),
		true, newLogFileSize, newIndexFileSize, true)
	if err != nil {
		return fmt.Errorf("create new active segment [%s] failed: %s\n",
			newFileBaseName, err.Error())
	}

	// 将新的active segment绑定到当前对象
	log.activeSegment = newActiveSegment
	newActiveFile := &baseFileInfo{newFileBaseName,
		int64(newIndexFileSize), int64(newLogFileSize)}
	log.activeFile = newActiveFile

	return nil
}

/*
写data到active segment.
如当前active segment剩余空间不够写入足够的data, 则关闭当前active segment变为read only模式,
再创建一个新的segment作为active segment.
*/
func (log *DiskLog) AppendBytes(data []byte, length int) (int, error) {
	if len(data) == 0 || length == 0 {
		return 0, utils.ZeroLengthError
	}

	for {
		err := log.activeSegment.AppendBytes(data, length)
		if err == nil {
			return length, nil
		}

		// 如果返回log文件或index文件荣容量不够的错误
		// 则关闭当前active segment, 再创建一个新的segment作为active segment
		if err == utils.LogFileRemainSizeSmall || err == utils.IndexFileRemainSizeSmall {
			err = log.RegenerateActiveSegment()
			if err != nil {
				return 0, err
			}
		}

		if err != nil {
			return 0, err
		}
	}
}

func (log *DiskLog) compareSegmentEntry(position int, startOffset int) int {
	segment := log.segments[position]
	if startOffset >= segment.startOffset && startOffset <= segment.currentOffset {
		return 0
	}

	if startOffset > segment.startOffset {
		return 1
	}

	if startOffset < segment.startOffset {
		return -1
	}

	return -1
}

// 根据target在当前目录下搜索
// 返回参数:
// 1. 目标segment在segment列表中的index
// 2. 目标segment指针
// 3. 目标segment中log文件的开始读取位置
// 4. 错误信息
func (log *DiskLog) Search(target int) (int, *LogIndexSegment, int, error) {
	// 首选判断是否在active segment范围内
	// 如果是就直接在active segment中查找
	if target >= log.activeSegment.startOffset {
		if target > log.activeSegment.currentOffset {
			return -1, nil, -1, utils.TargetGreatThanCommitted
		}

		result, _ := log.activeSegment.SearchIndex(target)
		if result < 0 {
			return -1, nil, -1, utils.TargetNotFound
		}
		filePos := log.activeSegment.GetIndexEntryValue(result)
		return len(log.segments) - 1, &log.activeSegment, int(filePos), nil
	}

	// 在read only的segments中查找
	binarySearch := func(begin, end int) (int, int) {
		var lo = begin
		var hi = end
		for {
			if lo >= hi {
				break
			}

			mid := int(math.Ceil(float64(hi)/2.0 + float64(lo)/2.0))
			compareResult := log.compareSegmentEntry(mid, target)
			if compareResult > 0 {
				lo = mid
			} else if compareResult < 0 {
				hi = mid - 1
			} else {
				return mid, mid
			}
		}

		var upperBound int
		if lo == len(log.segments)-1 {
			upperBound = -1
		} else {
			upperBound = lo + 1
		}

		return lo, upperBound
	}

	pos, _ := binarySearch(0, len(log.segments)-1)
	segment, err := log.GetSegment(pos)
	if err != nil {
		return -1, nil, -1, err
	}

	logFilePos := segment.Search(target)
	if logFilePos < 0 {
		return -1, nil, -1, utils.TargetNotFound
	} else {
		return pos, segment, logFilePos, nil
	}
}

/*
读取startOffset到endOffset范围的数据到socketFD.
如果readSize大于0, 则从startOffset开始读取指定大小的数据
*/
func ReadDataToSock(startOffset, length, readSize, sockFD int) error {
	return nil
}
