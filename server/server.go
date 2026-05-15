package server

import (
	"context"
	"github.com/DeleteElf/network-quic/streams"
	"github.com/DeleteElf/network-quic/utils"
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"strconv"
	"syscall"
	"time"
)

const MaxStreamCount = 6

func newUdpSocketServer(addr string) (net.PacketConn, error) {
	var err error
	config := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			err := c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
			return err
		},
	}
	config.SetMultipathTCP(false)
	conn, err := config.ListenPacket(context.Background(), streams.STREAM_NETWORK_UDP, addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type Stream struct {
	Info   *streams.StreamInfo
	Stream *quic.Stream
	Server *Server
}

type Socket struct {
	Id      string
	Streams [MaxStreamCount]*quic.Stream
	streams.StreamChannelObject
}

func (s *Socket) OnClosing() bool {
	for idx, stream := range s.Streams {
		if stream != nil {
			slog.Info("正在关闭流", slog.String("id", s.Id), slog.Int("chn", idx))
			_ = stream.Close()
		}
		s.Streams[idx] = nil
	}
	return true
}

func (s *Socket) OnClosed() {
	slog.Debug("Socket已经关闭", slog.String("id", s.Id))
}

type Server struct {
	isAgent        bool
	netConn        net.PacketConn
	listener       *quic.Listener
	sockets        map[string]*Socket
	IsClosed       bool
	onCloseHandler streams.OnCloseHandler
	streams.Closeable

	OnAccept chan string
}

// NewServer 创建新的服务实例，根据设置的地址监听
func NewServer(address string, isAgent bool) *Server {
	netConn, err := newUdpSocketServer(address)
	if err != nil {
		slog.Error("创建socket服务失败！", slog.Any("err", err))
		return nil
	}
	svr := &Server{
		isAgent:  isAgent,
		netConn:  netConn,
		OnAccept: make(chan string),
		sockets:  make(map[string]*Socket),
	}
	svr.SetOnCloseHandler(svr)
	svr.IsClosed = false
	return svr
}

// NewServerByPort 创建新的服务实例，并默认监听 0.0.0.0
func NewServerByPort(port int, isAgent bool) *Server {
	return NewServer("0.0.0.0:"+strconv.Itoa(port), isAgent)
}

func (s *Server) OnClosing() bool {
	slog.Debug("正在关闭基础连接")
	// 服务端先关闭，避免监听无法关闭
	if s.netConn != nil {
		_ = s.netConn.Close()
		s.netConn = nil
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	slog.Debug("正在关闭socket")
	for key, _ := range s.sockets {
		_ = s.CloseSocket(key)
	}
	s.sockets = nil
	slog.Debug("正在关闭Accept")
	close(s.OnAccept)
	return true
}

func (s *Server) OnClosed() {
	slog.Debug("服务端已经关闭")
}

// SetOnCloseHandler 设置关闭时需要执行的句柄
func (s *Server) SetOnCloseHandler(handler streams.OnCloseHandler) {
	s.onCloseHandler = handler
}

// Close 关闭流对象
func (s *Server) Close() {
	if !s.IsClosed { //防止多次执行
		s.IsClosed = true
		//开始释放资源
		if s.onCloseHandler != nil {
			if s.onCloseHandler.OnClosing() {
				s.onCloseHandler.OnClosed()
			}
		}
	}
}

func (s *Server) StartListen() {
	tlsConfig := utils.GenTLSConfig()
	quicConfig := &quic.Config{
		// MaxIncomingStreams: 0xffffffffffff, // 最大默认stream输入，默认100
		HandshakeIdleTimeout:    5 * time.Second,  // 默认5s
		MaxIdleTimeout:          30 * time.Second, // 默认30s
		KeepAlivePeriod:         3 * time.Second,  // 建议是 MaxIdleTimeout 的一半，或者更小的值
		InitialPacketSize:       1500,             //初始包大小
		DisablePathMTUDiscovery: true,
		Allow0RTT:               true,
	}

	var err error
	s.listener, err = quic.Listen(s.netConn, tlsConfig, quicConfig)
	if err != nil {
		slog.Error("启动服务监听发生错误！", slog.Any("err", err))
		return
	}
	slog.Info("服务启动监听", slog.Any("addr", s.netConn.LocalAddr()))
	for {
		if s.IsClosed { //已经关闭则退出
			break
		}
		if s.listener == nil {
			break
		}
		quicConn, err := s.listener.Accept(context.TODO())
		if s.IsClosed { //不再接受新的连接
			break
		}
		if err != nil {
			slog.Warn("接入连接失败", slog.Any("err", err))
			continue
		}
		slog.Info("接入一个新的连接", slog.Any("addr", quicConn.RemoteAddr()))
		go s.acceptConnection(quicConn)
	}
	slog.Info("服务停止监听")
}

func (s *Server) acceptConnection(quicConn *quic.Conn) {
	defer func() {
		slog.Info("连接断开", slog.Any("addr", quicConn.RemoteAddr()))
		_ = quicConn.CloseWithError(0, "other")
	}()

	for {
		if s.IsClosed {
			break
		}
		stream, err := quicConn.AcceptStream(context.TODO())
		if err != nil {
			slog.Error("接入一个新的流发生错误", slog.Any("err", err))
			return
		}
		go s.processStream(stream)
	}
}

func (s *Server) processStream(stream *quic.Stream) {
	streamId := stream.StreamID()
	info, err := streams.ReadStreamInfo(stream)
	if err != nil {
		slog.Error("获取流信息失败", slog.Any("streamId", streamId), slog.Any("err", err))
		_ = streams.CloseStream(stream)
		return
	}
	if err := streams.ValidateStreamInfo(info); err != nil {
		slog.Warn("无效的流信息", slog.Any("err", err))
		_ = streams.CloseStream(stream)
		return
	}
	if info.Index < 0 || info.Index >= MaxStreamCount {
		slog.Error("无效的通道", slog.Any("chn", info.Index))
		_ = streams.CloseStream(stream)
		return
	}
	slog.Info("启动通道通讯", slog.Int("chn", info.Index), slog.Any("streamId", streamId))
	if s.sockets[info.Id] == nil {
		sock := &Socket{
			Id: info.Id,
			StreamChannelObject: streams.StreamChannelObject{
				IsClosed: false,
			},
		}
		sock.SetOnCloseHandler(sock)
		sock.CreateChannels(1)
		s.sockets[info.Id] = sock
		s.OnAccept <- info.Id
	}
	socket := s.sockets[info.Id]
	socket.Streams[info.Index] = stream
	go socket.HandleChannelStreamData(socket.StreamChannels[0], info.Index, stream)
}
func (s *Server) CloseSocket(id string) error {
	if s.sockets[id] != nil {
		s.sockets[id].Close()
		delete(s.sockets, id)
	}
	return nil
}
func (s *Server) GetSocket(id string) *Socket {
	if s.sockets[id] != nil {
		return s.sockets[id]
	}
	return nil
}
