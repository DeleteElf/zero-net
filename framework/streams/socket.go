package streams

import (
	"errors"
	"github.com/DeleteElf/network-quic/framework"
	"github.com/quic-go/quic-go"
	"log/slog"
	"sync"
)

type MessageCallbackFunc func(string)

// Socket 流基础对象
type Socket struct {
	Id             string
	StreamChannels []*StreamChannel
	ChannelCount   int

	OnDisconnect MessageCallbackFunc

	framework.CloseableObject
	StreamChannelOperating

	channelEditLock sync.Mutex
}

func NewSocket(id string, channelCount int, onDisconnect MessageCallbackFunc) *Socket {
	sock := &Socket{
		Id:           id,
		ChannelCount: channelCount,
	}
	sock.IsClosed = false
	sock.SetOnCloseHandler(sock)
	sock.OnDisconnect = onDisconnect
	sock.CreateChannels(channelCount)
	return sock
}

func (s *Socket) OnClosing() bool {
	s.channelEditLock.Lock()
	defer s.channelEditLock.Unlock()
	for i := 0; i < len(s.StreamChannels); i++ {
		if s.StreamChannels[i] != nil {
			s.StreamChannels[i].Close()
			s.StreamChannels[i] = nil
		}
	}
	s.ChannelCount = 0
	s.StreamChannels = make([]*StreamChannel, 0) //清空切片
	return true
}

func (s *Socket) OnClosed() {
	slog.Debug("socket 已经退出！", slog.String("id", s.Id))
	if s.OnDisconnect != nil {
		s.OnDisconnect(s.Id)
	}
}

// CreateChannels 创建通道
func (s *Socket) CreateChannels(count int) {
	s.StreamChannels = make([]*StreamChannel, count) //创建通道列表切片
	for i := 0; i < count; i++ {
		s.StreamChannels[i] = NewStreamChannel(s.Id, i) //make(chan StreamChannelData, 3) //创建通道实例
		s.StreamChannels[i].OnDisconnect = func(id string, index int) {
			slog.Debug("socket的通道断开连接！", slog.Int("index", index))
			{
				s.channelEditLock.Lock()
				defer s.channelEditLock.Unlock()
				if index < len(s.StreamChannels) && s.StreamChannels[index] != nil {

					s.StreamChannels[index].Close()
					s.StreamChannels[index] = nil
				}
			}
			if s.OnDisconnect != nil {
				finded := false
				for _, channel := range s.StreamChannels {
					if channel != nil {
						finded = true
					}
				}
				if !finded {
					slog.Debug("socket的通道已全部断开连接！")
					s.Close()
				}
			}
		}
	}
	s.ChannelCount = count
}

// HandleChannelStreamData 从通道接收流的数据
func (s *Socket) HandleChannelStreamData(channelId int, stream *quic.Stream) {
	s.StreamChannels[channelId].HandleChannelStreamData(stream)
}

func (s *Socket) ReceiveDataToBuffer(channelId int) (bool, error) {
	if len(s.StreamChannels) == 0 {
		return false, errors.New("当前socket的通道数为0！")
	}
	if channelId >= s.ChannelCount {
		return false, errors.New("超过通道允许范围！")
	}
	if s.StreamChannels[channelId] != nil {
		return s.StreamChannels[channelId].ReceiveDataToBuffer(), nil
	}
	return false, errors.New("通道未初始化！")
}

func (s *Socket) Send(channelId int, data []byte) (bool, error) {
	if s.IsClosed {
		return false, nil
	}
	if channelId >= s.ChannelCount {
		return false, errors.New("超过通道允许范围！")
	}
	if s.StreamChannels[channelId] == nil {
		return false, errors.New("通道未初始化！")
	}
	return s.StreamChannels[channelId].Send(data)
}
