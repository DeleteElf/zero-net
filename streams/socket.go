package streams

import (
	"github.com/DeleteElf/network-quic/framework"
	"github.com/DeleteElf/network-quic/framework/utils"
	"github.com/quic-go/quic-go"
	"log/slog"
)

// SocketObject 流基础对象
type SocketObject struct {
	Id             string
	StreamChannels []*StreamChannel
	IsClosed       bool
	Count          int

	framework.CloseableObject
	StreamChannelOperating
}

func (s *SocketObject) OnClosing() bool {
	for i := 0; i < len(s.StreamChannels); i++ {
		if s.StreamChannels[i] != nil {
			s.StreamChannels[i].Close()
			s.StreamChannels[i] = nil
		}
	}
	s.StreamChannels = make([]*StreamChannel, 0) //清空切片
	return true
}

func (s *SocketObject) OnClosed() {
	slog.Debug("socket 已经退出！", slog.String("id", s.Id))
}

// CreateChannels 创建通道
func (s *SocketObject) CreateChannels(count int) {
	s.StreamChannels = make([]*StreamChannel, count) //创建通道列表切片
	for i := 0; i < count; i++ {
		s.StreamChannels[i] = NewStreamChannel(s.Id, i) //make(chan StreamChannelData, 3) //创建通道实例
	}
}

// HandleChannelStreamData 从通道接收流的数据
func (s *SocketObject) HandleChannelStreamData(channelId int, stream *quic.Stream) {
	s.StreamChannels[channelId].HandleChannelStreamData(stream)
}

func (s *SocketObject) ReceiveDataToBuffer(channelIndex int) bool {
	if len(s.StreamChannels) == 0 {
		return false
	}
	if s.StreamChannels[channelIndex] != nil {
		s.StreamChannels[channelIndex].ReceiveDataToBuffer()
		return true
	}
	return false
}

func (s *SocketObject) Send(stream *quic.Stream, data []byte) (bool, error) {
	if s.IsClosed {
		return false, nil
	}
	//s.wg.Add(1)
	//defer s.wg.Done()
	err := utils.WriteStreamByHeaderUShort(stream, data)
	return err == nil, err
}
