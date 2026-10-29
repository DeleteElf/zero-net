package server

import (
	"context"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"strings"
	"sync"
)

const MaxStreamCount = 6

type Server struct {
	isAgent  bool
	listener *quic.Listener
	Sockets  map[string]*network.Socket
	lock     sync.Mutex

	OnAcceptSocket       network.SocketCallbackFunc
	OnSocketDisConnected network.SocketCallbackFunc

	network.Endpoint
}

// NewServerByAddress 创建新的服务实例，根据设置的地址监听
func NewServerByAddress(address string) *Server {
	netConn, err := network.NewUdpSocketServer(address)
	if err != nil {
		slog.Error("创建socket服务失败！", slog.Any("err", err))
		return nil
	}
	return NewServer(netConn, false)
}

// NewServer 创建新的服务实例
func NewServer(conn net.PacketConn, isAgent bool) *Server {
	svr := &Server{
		isAgent: isAgent,
		Sockets: make(map[string]*network.Socket),
	}
	svr.FecBlockSize = network.NetMtuPacketSize
	//svr.FecPercentage = 30
	svr.FecMinRequiredPackets = 2
	svr.NetConn = conn
	svr.SetOnCloseHandler(svr)
	return svr
}

func (s *Server) OnClosing() bool {
	slog.Debug("正在关闭服务端")
	// 服务端先关闭，避免监听无法关闭
	if s.NetConn != nil {
		_ = s.NetConn.Close()
		s.NetConn = nil
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	for key, _ := range s.Sockets {
		_ = s.CloseSocket(key)
	}
	s.Sockets = nil
	return true
}

func (s *Server) OnClosed() error {
	slog.Debug("服务端已经关闭")
	return nil
}

func (s *Server) StartListen(onDisconnect network.SocketCallbackFunc) {
	s.StartListenWithConn(onDisconnect, nil)
}
func (s *Server) StartListenWithConn(onDisconnect network.SocketCallbackFunc, conn net.PacketConn) {
	if conn != nil {
		s.NetConn = conn
	}
	err := s.SetDSCP(s.NetConn, 46)
	if err != nil {
		slog.Error("设置qos出错！", slog.Any("err", err))
		return
	}
	// 4. 构建 quic.Transport（复用刚刚创建的底层 conn）
	tr := &quic.Transport{
		Conn: s.NetConn,
	}
	tlsConfig := utils.GenTLSConfig()
	s.SetDefaultConfig()
	s.listener, err = tr.Listen(tlsConfig, s.QuicConfig)

	//s.listener, err = quic.Listen(s.NetConn, tlsConfig, quicConfig)
	if err != nil {
		slog.Error("启动服务监听发生错误！", slog.Any("err", err))
		return
	}
	slog.Info("服务启动监听", slog.Any("addr", s.NetConn.LocalAddr()))
	//s.IsInQuic = true
	for {
		if s.IsClosed() { //已经关闭则退出
			break
		}
		if s.listener == nil {
			break
		}
		s.QuicConn, err = s.listener.Accept(context.TODO())
		if s.IsClosed() { //不再接受新的连接
			break
		}
		if err != nil {
			slog.Warn("接入连接失败", slog.Any("err", err))
			break
		}
		slog.Info("接入一个新的连接", slog.Any("addr", s.QuicConn.RemoteAddr()))
		go s.acceptConnection(s.QuicConn, onDisconnect)
	}
	slog.Info("服务停止监听")
}

func (s *Server) acceptConnection(quicConn *quic.Conn, onDisconnect network.SocketCallbackFunc) {
	defer func() {
		slog.Info("连接断开", slog.Any("addr", quicConn.RemoteAddr()))
		_ = quicConn.CloseWithError(0, "other")
	}()

	for {
		if s.IsClosed() {
			break
		}
		stream, err := quicConn.AcceptStream(context.TODO())
		if err != nil {
			if !strings.HasPrefix(err.Error(), "Application error 0x0 (remote)") {
				slog.Error("接入一个新的流发生错误", slog.Any("err", err))
			}
			return
		}
		go s.processStream(quicConn, stream, onDisconnect)
	}
}

func (s *Server) processStream(quicConn *quic.Conn, stream *quic.Stream, onDisconnect network.SocketCallbackFunc) {
	streamId := stream.StreamID()
	info, err := network.ReadStreamInfo(stream)
	if err != nil {
		slog.Error("获取流信息失败", slog.Any("streamId", streamId), slog.Any("err", err))
		_ = network.CloseStream(stream)
		return
	}
	if err := network.ValidateStreamInfo(info); err != nil {
		slog.Warn("无效的流信息", slog.Any("err", err))
		_ = network.CloseStream(stream)
		return
	}
	if info.ChannelIndex < 0 || info.ChannelIndex >= MaxStreamCount {
		slog.Error("无效的通道", slog.Int("chn", info.ChannelIndex))
		_ = network.CloseStream(stream)
		return
	}
	slog.Info("启动通道通讯", slog.Int("chn", info.ChannelIndex), slog.Any("streamId", streamId), slog.String("clientId", info.Id))
	s.lock.Lock()
	if s.Sockets[info.Id] == nil {
		socket := network.NewSocket(info.Id, info.ChannelCount, s.QuicConfig.InitialPacketSize, func(sock *network.Socket) {
			if s.Sockets[sock.Id] != nil {
				s.Sockets[sock.Id] = nil
				delete(s.Sockets, sock.Id)
			}
			if onDisconnect != nil {
				onDisconnect(sock)
			}
		})
		socket.StreamConfigs = make([]network.StreamConfig, info.ChannelCount) //先创立基础通道，详细信息，等实际接入在获取
		socket.CreateChannels()
		socket.Conn = quicConn
		s.Sockets[info.Id] = socket
		if s.QuicConfig.EnableDatagrams {
			if socket.PacketPool == nil {
				socket.PacketPool = socket.CreatePacketPool(s.QuicConfig.InitialPacketSize)
			}
		}
		if s.OnAcceptSocket != nil {
			s.OnAcceptSocket(socket)
		}
		if s.QuicConfig.EnableDatagrams {
			go socket.HandleChannelStreamDatagram()
		}
	}
	socket := s.Sockets[info.Id]
	if s.SupportFec && s.QuicConfig.EnableDatagrams {
		err = socket.InitFecParam(info.ChannelIndex)
	}
	s.lock.Unlock()
	if err != nil {
		return
	}
	go socket.HandleChannelStream(info.ChannelIndex, stream)
}
func (s *Server) CloseSocket(id string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.Sockets[id] != nil {
		_ = s.Sockets[id].Close()
		s.Sockets[id] = nil
		delete(s.Sockets, id)
	}
	return nil
}
func (s *Server) GetSocket(id string) *network.Socket {
	if s.Sockets[id] != nil {
		return s.Sockets[id]
	}
	return nil
}
