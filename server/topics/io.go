package topics

import (
	"fmt"
	"github.com/shelmesky/gms/server/common"
	disklog "github.com/shelmesky/gms/server/log"
	"github.com/shelmesky/gms/server/utils"
	log "github.com/sirupsen/logrus"
	"io"
	"strconv"
)

func SendFileToSocket(segment *disklog.LogIndexSegment, client *common.Client, pos, length int64) error {
	_, err := segment.Log.File.Seek(pos, 0)
	written, err := client.Conn.ReadFrom(io.LimitReader(segment.Log.File, length))
	if written == 0 || err != nil {
		//err = client.Conn.Close()
		if err != nil {
			return utils.CloseConnError
		}
		return utils.WrittenNotEnoughError
	}

	return nil
}

/*
从Log对象中批量读取消息
log: 管理磁盘日志的对象
client: 客户端连接对象
target: 开始读取的offset
length: 希望读取几个消息
*/
func BatchRead(diskLog *disklog.DiskLog, client *common.Client, targetOffset, length int) error {
	var bytesRead int
	var originPos int64

	// 根据target参数在Log对象中搜索是否存在对应的offset
	// 获取segment即对应的文件段对象
	// segmentPos即文件段对象在所有segment list中的索引
	// logFilePos即target offset在对应segment开始读取的位置
	segmentPos, segment, logFilePos, err := diskLog.Search(targetOffset)
	if err != nil {
		return err
	}

	// 如果logFilePos小于0,说明在index文件中找到target offset
	if logFilePos < 0 {
		return fmt.Errorf("cant find offset\n")
	}

	// 获取第一条消息的长度
	messageLength, err := segment.Log.ReadUInt32(logFilePos + 4)
	if err != nil {
		return err
	}

	// 发送数据
	err = SendFileToSocket(segment, client, int64(logFilePos), int64(messageLength)+8)
	if err != nil {
		return err
	}

	// 将消息指针向前移动4+4+MessageLength
	logFilePos += 8
	logFilePos += int(messageLength)

	// 保存此时第一次读取到的POS位置
	// 进入循环之前保存
	originPos = int64(logFilePos)

	// 减去上面读取的一条记录
	length -= 1

	if length == 0 {
		return nil
	}

	readCounter := 0

	/*
		logFilePos: 跟踪当前log文件读取的位置
		originPos: 用于sendfile时传递开始的位置
		bytesRead: 用于从start offset到文件末尾或者读够了数量时，记录总共读取了多少数据.
	*/

	for {
		// 读到足够数量的消息, 退出
		if readCounter >= length {
			err = SendFileToSocket(segment, client, originPos, int64(bytesRead))
			if err != nil {
				return err
			}
			break
		}

		// 消息的长度
		messageLength, err := segment.Log.ReadUInt32(logFilePos + 4)
		if err != nil {
			return err
		}

		// 说明读到了log文件末尾
		// 切换下一个segment并从文件开始处读
		if messageLength == 0 {
			// 将当前segment的内容发送到client
			err = SendFileToSocket(segment, client, originPos, int64(bytesRead))
			if err != nil {
				return err
			}

			// 重置originPos，因为切换了新的文件
			originPos = 0
			bytesRead = 0

			// 切换到下个segment继续读
			segmentPos += 1
			segment, err = diskLog.GetSegment(segmentPos)
			// 如果返回错误,说明已经读取完所有的segment
			if err != nil {
				return err
			}
			logFilePos = 0
			continue
		} else {
			//未读到文件尾，继续读取

			// 增加文件读取位置的指针
			logFilePos += 8
			logFilePos += int(messageLength)

			bytesRead += 8
			bytesRead += int(messageLength)

			// 读取的数量自增1
			readCounter += 1
		}
	}

	return nil
}

func ReadMessage(client *common.Client, topicName, partitionIndex string, targetOffset, count uint32) error {
	// 必须提供长度大于0的topic名字
	if len(topicName) > 0 {
		// 根据名字获得topic对象
		topic := TopicManager.GetTopic(topicName)

		// 如果根据topic名字能找到Topic对象
		if topic != nil {
			// 提供了partition number
			if len(partitionIndex) > 0 {
				// partition序号
				partitionNum, err := strconv.Atoi(partitionIndex)
				if err != nil {

				}

				// 根据partition序号找到Partition
				partition := topic.GetPartition(partitionNum)

				// 如果请求参数中的targetOffset和分区目前一样大
				// 则返回错误
				log.Debugf("ReadMessage() targetOffset: [%d],  currentOffset: [%d]\n",
					targetOffset, partition.GetCurrentOffset())
				if int(targetOffset) > partition.GetCurrentOffset() {

					return utils.OffsetBigError
				}

				// Partition的Log对象
				Log := partition.GetLog()
				// 使用Log对象批量读取消息
				err = BatchRead(Log, client, int(targetOffset), int(count))

				if err != nil {
					return err
				}
				return nil
			} else {
				// TODO: 未提供partition number
			}

		} else {
			return utils.ServerError
		}

	}

	return utils.ParameterTopicMissed
}
