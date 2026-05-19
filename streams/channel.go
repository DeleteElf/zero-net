package streams

import "C"
import (
	"context"
	"errors"
	"github.com/DeleteElf/network-quic/framework"
	"github.com/DeleteElf/network-quic/framework/utils"
	"github.com/quic-go/quic-go"
	"io"
	"log/slog"
	"time"
)

type StreamChannelOperating interface {
	CreateChannels(count int)
	HandleChannelStreamData(channel chan StreamChannelData, channelId int, stream *quic.Stream)
	Send(channelId int, data []byte) (bool, error)
}

// StreamChannelData 流通道数据结构
type StreamChannelData struct {
	ClientId  string
	ChannelId int
	Offset    int
	Data      []byte
}

type StreamChannel struct {
	Channel   chan StreamChannelData
	ClientId  string
	ChannelId int
	Cancel    context.CancelFunc
	Done      bool
	Buffer    *StreamChannelData

	Stream *quic.Stream

	framework.CloseableObject
}

func NewStreamChannel(id string, index int) *StreamChannel {
	slog.Debug("正在创建通道", slog.String("id", id), slog.Int("ChannelId", index))
	sc := &StreamChannel{
		Channel:   make(chan StreamChannelData),
		ClientId:  id,
		ChannelId: index,
		Buffer:    &StreamChannelData{},
		CloseableObject: framework.CloseableObject{
			IsClosed: false,
		},
	}
	sc.SetOnCloseHandler(sc)
	return sc
}

func (sc *StreamChannel) OnClosing() bool {
	if sc.Cancel != nil {
		sc.Cancel()
	}
	sc.Cancel = nil
	if sc.Stream != nil {
		_ = sc.Stream.Close()
		sc.Stream = nil
	}
	count := 100
	for i := 0; i < count; i++ {
		if !sc.Done {
			time.Sleep(time.Millisecond)
			continue
		}
		break
	}
	return true
}

func (sc *StreamChannel) OnClosed() {
	slog.Debug("检测到通道已经退出！", slog.String("id", sc.ClientId), slog.Int("通道", sc.ChannelId))
	sc.Buffer = nil
	sc.Channel = nil
}

// HandleChannelStreamData 从通道接收流的数据
func (sc *StreamChannel) HandleChannelStreamData(stream *quic.Stream) {
	sc.Stream = stream
	_, sc.Cancel = context.WithCancel(sc.Stream.Context())
	defer func() {
		if !sc.Done && sc.Channel != nil {
			close(sc.Channel)
			sc.Channel = nil
		}
		sc.Done = true
	}()
	slog.Debug("完成流与通道的对接，开始读取通道数据", slog.Int("channel", sc.ChannelId))
	for {
		if sc.IsClosed {
			return
		}
		buf, err := utils.ReadStreamByHeaderUShort(sc.Stream)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) { //如果是读取超时，我们就继续即可
				continue
			} else if err != io.EOF {
				slog.Error("通道读取失败！", slog.Int("ChannelId", sc.ChannelId), slog.Any("err", err))
			} else {
				slog.Error("通道流已经结束！", slog.Int("ChannelId", sc.ChannelId))
			}
			return
		}
		if sc.IsClosed {
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
			ClientId:  sc.ClientId,
			ChannelId: sc.ChannelId,
			Offset:    0,
			Data:      buf,
		}
	}
}

func (sc *StreamChannel) ReceiveDataToBuffer() bool {
	if sc.Buffer == nil { //当前缓存没有工作时
		buffer, ok := <-sc.Channel
		if !ok {
			sc.Close()
			return ok
		}
		sc.Buffer = &buffer
	}
	return true
}

func (sc *StreamChannel) Send(data []byte) (bool, error) {
	if sc.IsClosed {
		return false, nil
	}
	if sc.Stream == nil {
		return false, nil
	}
	err := utils.WriteStreamByHeaderUShort(sc.Stream, data)
	return err == nil, err
}
