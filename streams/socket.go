package streams

import (
	"github.com/DeleteElf/network-quic/framework"
	"github.com/quic-go/quic-go"
	"log/slog"
)

// Socket 流基础对象
type Socket struct {
	Id             string
	StreamChannels []*StreamChannel
	ChannelCount   int

	framework.CloseableObject
	StreamChannelOperating
}

func NewSocket(id string, channelCount int) *Socket {
	sock := &Socket{
		Id:           id,
		ChannelCount: channelCount,
	}
	sock.IsClosed = false
	sock.SetOnCloseHandler(sock)
	sock.CreateChannels(channelCount)
	return sock
}

func (s *Socket) OnClosing() bool {
	for i := 0; i < len(s.StreamChannels); i++ {
		if s.StreamChannels[i] != nil {
			s.StreamChannels[i].Close()
			s.StreamChannels[i] = nil
		}
	}
	s.StreamChannels = make([]*StreamChannel, 0) //清空切片
	return true
}

func (s *Socket) OnClosed() {
	slog.Debug("socket 已经退出！", slog.String("id", s.Id))
}

// CreateChannels 创建通道
func (s *Socket) CreateChannels(count int) {
	s.StreamChannels = make([]*StreamChannel, count) //创建通道列表切片
	for i := 0; i < count; i++ {
		s.StreamChannels[i] = NewStreamChannel(s.Id, i) //make(chan StreamChannelData, 3) //创建通道实例
	}
	s.ChannelCount = count
}

// HandleChannelStreamData 从通道接收流的数据
func (s *Socket) HandleChannelStreamData(channelId int, stream *quic.Stream) {
	s.StreamChannels[channelId].HandleChannelStreamData(stream)
}

func (s *Socket) ReceiveDataToBuffer(channelIndex int) bool {
	if len(s.StreamChannels) == 0 {
		return false
	}
	if s.StreamChannels[channelIndex] != nil {
		s.StreamChannels[channelIndex].ReceiveDataToBuffer()
		return true
	}
	return false
}

func (s *Socket) Send(channelId int, data []byte) (bool, error) {
	if s.IsClosed {
		return false, nil
	}
	return s.StreamChannels[channelId].Send(data)
}
