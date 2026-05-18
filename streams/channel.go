package streams

import "C"
import (
	"context"
	"errors"
	"github.com/DeleteElf/network-quic/utils"
	"github.com/quic-go/quic-go"
	"io"
	"log/slog"
	"sync"
	"time"
)

// StreamChannelData 流通道数据结构
type StreamChannelData struct {
	ChannelId int
	Offset    int
	Data      []byte
}

type StreamChannel struct {
	Channel chan StreamChannelData
	//Closed  chan bool
	Id         int
	Cancel     context.CancelFunc
	Done       bool
	OneChannel bool
}

func NewStreamChannel(id int) *StreamChannel {
	return &StreamChannel{
		Channel: make(chan StreamChannelData),
		Id:      id,
		//Closed:  make(chan bool),
	}
}

func (sc *StreamChannel) Close(duration time.Duration) {
	//sc.Closed <- true
	if sc.Cancel != nil {
		sc.Cancel()
	}
	sc.Cancel = nil
	count := int(duration.Milliseconds())
	for i := 0; i < count; i++ {
		if !sc.Done {
			time.Sleep(time.Millisecond)
			continue
		}
		break
	}
	slog.Info("检测到通道已经退出！", slog.Int("通道", sc.Id))
	sc.Channel = nil
	//close(sc.Channel)
	//close(sc.Closed)
}

type OnCloseHandler interface {
	OnClosing() bool
	OnClosed()
}

// Closeable 可关闭对象
type Closeable interface {
	Close()
	SetOnCloseHandler(handler OnCloseHandler)
}

type StreamChannelOperating interface {
	CreateChannels(count int)
	HandleChannelStreamData(channel chan StreamChannelData, channelId int, stream *quic.Stream)
	Send(channelId int, data []byte) (bool, error)
}

// StreamChannelObject 流基础对象
type StreamChannelObject struct {
	wg sync.WaitGroup //用于保证通道内的数据传输完成！

	StreamChannels []*StreamChannel
	IsClosed       bool

	CurrentBuffers []*StreamChannelData

	onCloseHandler OnCloseHandler
	Closeable
	StreamChannelOperating
}

// SetOnCloseHandler 设置关闭时需要执行的句柄
func (o *StreamChannelObject) SetOnCloseHandler(handler OnCloseHandler) {
	o.onCloseHandler = handler
}

// Close 关闭流对象
func (o *StreamChannelObject) Close() {
	if !o.IsClosed { //防止多次执行
		o.IsClosed = true
		//为了解决各个通道不能及时关闭的问题，这里向所有通道发送一条关闭的命令
		o.wg.Wait() //等待发送和接收完成
		//开始释放资源
		if o.onCloseHandler != nil {
			if o.onCloseHandler.OnClosing() {
				for i := 0; i < len(o.StreamChannels); i++ {
					if o.StreamChannels[i] != nil {
						o.StreamChannels[i].Cancel()
						o.StreamChannels[i].Close(time.Second)
						o.StreamChannels[i] = nil
					}

					if o.CurrentBuffers[i] != nil {
						o.CurrentBuffers[i] = nil
					}
				}
				o.CurrentBuffers = make([]*StreamChannelData, 0)
				o.StreamChannels = make([]*StreamChannel, 0) //清空切片
				o.onCloseHandler.OnClosed()
			}
		}
	}
}

// CreateChannels 创建通道
func (o *StreamChannelObject) CreateChannels(count int) {
	o.StreamChannels = make([]*StreamChannel, count)     //创建通道列表切片
	o.CurrentBuffers = make([]*StreamChannelData, count) //创建缓存通道列表
	for i := 0; i < count; i++ {
		o.StreamChannels[i] = NewStreamChannel(i) //make(chan StreamChannelData, 3) //创建通道实例
		if count == 1 {
			o.StreamChannels[i].OneChannel = true
		}
	}
}

// HandleChannelStreamData 从通道接收流的数据
func (o *StreamChannelObject) HandleChannelStreamData(sc *StreamChannel, channelId int, stream *quic.Stream) {
	_, sc.Cancel = context.WithCancel(stream.Context())
	defer func() {
		if !sc.Done && sc.Channel != nil {
			if !sc.OneChannel || channelId == 0 {
				close(sc.Channel)
			}
			sc.Channel = nil
		}
		sc.Done = true
	}()
	slog.Debug("开始读取通道数据", slog.Int("channel", channelId))

	for {
		if o.IsClosed {
			return
		}
		buf, err := utils.ReadStreamByHeaderUShort(stream)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) { //如果是读取超时，我们就继续即可
				continue
			} else if err != io.EOF {
				slog.Error("通道读取失败！", slog.Int("channel", channelId), slog.Any("err", err))
			} else {
				slog.Error("通道流已经结束！", slog.Int("channel", channelId))
			}
			return
		}
		if o.IsClosed {
			return
		}
		if len(buf) == 0 { //读取到0长度的数据包，我们认为是断开连接了
			return
		}
		if sc == nil {
			return
		}
		if sc.Channel == nil {
			return
		}
		sc.Channel <- StreamChannelData{
			ChannelId: channelId,
			Offset:    0,
			Data:      buf,
		}
	}
}

func (o *StreamChannelObject) ReceiveDataToBuffer(channelIndex int) bool {
	if len(o.CurrentBuffers) == 0 {
		return false
	}
	if o.CurrentBuffers[channelIndex] == nil { //当前缓存没有工作时
		buffer, ok := <-o.StreamChannels[channelIndex].Channel
		if !ok {
			o.Close()
			return ok
		}
		o.CurrentBuffers[channelIndex] = &buffer
	}
	return true
}

func (o *StreamChannelObject) Send(stream *quic.Stream, data []byte) (bool, error) {
	if o.IsClosed {
		return false, nil
	}
	//o.wg.Add(1)
	//defer o.wg.Done()
	err := utils.WriteStreamByHeaderUShort(stream, data)
	return err == nil, err
}
