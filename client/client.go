package client

import (
	"context"
	"errors"
	"github.com/DeleteElf/network-quic/framework"
	"github.com/DeleteElf/network-quic/framework/streams"
	"github.com/DeleteElf/network-quic/framework/utils"
	"log/slog"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

// Client 客户端
type Client struct {
	Id string
	//需要连接的服务端地址
	ServerAddress string
	netConn       net.PacketConn
	netAddr       net.Addr
	quicConn      *quic.Conn
	Socket        *streams.Socket
	framework.CloseableObject
}

// NewClient 创建客户端实例
func NewClient(addr string, id string) *Client {
	cli := &Client{
		ServerAddress: addr,
		Id:            id,
	}
	cli.IsClosed = false
	cli.SetOnCloseHandler(cli)
	return cli
}

func (cli *Client) CloseChannel(channelId int) bool {
	if cli.Socket != nil && len(cli.Socket.StreamChannels) > channelId &&
		cli.Socket.StreamChannels[channelId] != nil {
		cli.Socket.StreamChannels[channelId].Close()
		cli.Socket.StreamChannels[channelId] = nil
	}
	return true
}
func (cli *Client) OnClosing() bool {
	if cli.Socket != nil {
		cli.Socket.Close()
		cli.Socket = nil
	}
	if cli.quicConn != nil {
		_ = cli.quicConn.CloseWithError(0, "close")
	}
	if cli.netConn != nil {
		_ = cli.netConn.Close()
	}
	return true
}

func (cli *Client) OnClosed() {
	slog.Debug("客户端已经关闭")
}

func (cli *Client) Connect(channelCount int, networkType string, onDisconnect streams.SocketCallbackFunc) error {
	if networkType != "udp" {
		return errors.New("暂时只支持udp连接！")
	}
	var err error
	netConn, netAddr, err := streams.NewUdpSocketClient(cli.ServerAddress)
	if err != nil {
		slog.Error("客户端连接服务器失败", slog.Any("err", err))
		cli.Close()
		return err
	}
	return cli.ConnectToNet(channelCount, netConn, netAddr, onDisconnect)
}
func (cli *Client) ConnectToNet(channelCount int, conn net.PacketConn, addr net.Addr, onDisconnect streams.SocketCallbackFunc) error {
	if cli.Socket != nil {
		return errors.New("当前客户端已经连接！")
	}
	cli.netConn = conn
	cli.netAddr = addr
	cli.Socket = streams.NewSocket(cli.Id, channelCount, onDisconnect)
	tlsConfig := utils.GenTLSConfig()
	quicConfig := &quic.Config{
		MaxIncomingStreams:      0xffffffffffff,   // 最大默认stream输入，默认100
		HandshakeIdleTimeout:    5 * time.Second,  // 默认5s
		MaxIdleTimeout:          10 * time.Second, // 默认30s，我们这边设置成10秒
		KeepAlivePeriod:         3 * time.Second,  // 建议是 MaxIdleTimeout 的一半，或者更小的值
		InitialPacketSize:       1500,             //当前最大数据包一个基础包的大小
		DisablePathMTUDiscovery: true,
		Allow0RTT:               true,
		// EnableDatagrams:    true,
	}
	slog.Debug("正在建立远程连接", slog.Any("ServerAddress", cli.netAddr))
	quicConn, err := quic.Dial(context.TODO(), cli.netConn, cli.netAddr, tlsConfig, quicConfig)

	if err != nil {
		slog.Info("远程连接失败！", slog.Any("err", err))
		cli.Close()
		return err
	}
	cli.quicConn = quicConn
	info := streams.StreamInfo{
		Id:    cli.Id,
		Count: channelCount,
		Ts:    time.Now().Unix(),
	}
	for i := 0; i < channelCount; i++ {
		info.Index = i
		stream, err := streams.CreateStream(quicConn, info) //创建并打开流
		if err != nil {
			cli.Close()
			return err
		}
		go cli.Socket.HandleChannelStreamData(i, stream)
	}
	return nil
}
func (cli *Client) ConnectToAgent(channelCount int, conn net.PacketConn, addr net.Addr, onDisconnect streams.SocketCallbackFunc) {
	if cli.Socket != nil {
		return // errors.New("当前客户端已经连接！")
	}
	cli.netConn = conn
	cli.netAddr = addr

	tlsConfig := utils.GenTLSConfig()
	quicConfig := &quic.Config{
		MaxIncomingStreams:      0xffffffffffff,   // 最大默认stream输入，默认100
		HandshakeIdleTimeout:    5 * time.Second,  // 默认5s
		MaxIdleTimeout:          10 * time.Second, // 默认30s，我们这边设置成10秒
		KeepAlivePeriod:         3 * time.Second,  // 建议是 MaxIdleTimeout 的一半，或者更小的值
		InitialPacketSize:       1500,             //当前最大数据包一个基础包的大小
		DisablePathMTUDiscovery: true,
		Allow0RTT:               true,
		// EnableDatagrams:    true,
	}
	slog.Debug("正在通过代理建立远程连接", slog.Any("ServerAddress", cli.netAddr))
	quicConn, err := quic.Dial(context.TODO(), cli.netConn, cli.netAddr, tlsConfig, quicConfig)
	if err != nil {
		slog.Info("远程连接失败！", slog.Any("err", err))
		return
	}
	cli.quicConn = quicConn

	cli.Socket = streams.NewSocket(cli.Id, channelCount, onDisconnect)
	slog.Info("客户端连接成功！", slog.Int("通道数", cli.Socket.ChannelCount))
	info := streams.StreamInfo{
		Id:    cli.Id,
		Count: channelCount,
		Ts:    time.Now().Unix(),
	}
	for i := 0; i < channelCount; i++ {
		info.Index = i
		stream, err := streams.CreateStream(cli.quicConn, info) //创建并打开流
		if err != nil {
			cli.Close()
			return
		}
		go cli.Socket.HandleChannelStreamData(i, stream)
	}
}

func (cli *Client) processStream(stream *quic.Stream, onDisconnect streams.SocketCallbackFunc) {
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
	if info.Index < 0 || info.Index >= cli.Socket.ChannelCount {
		slog.Error("无效的通道", slog.Any("chn", info.Index))
		_ = streams.CloseStream(stream)
		return
	}
	slog.Info("启动通道通讯", slog.Int("chn", info.Index), slog.Any("streamId", streamId), slog.String("clientId", info.Id))
	go cli.Socket.HandleChannelStreamData(info.Index, stream)
}

func (cli *Client) waitConn() *quic.Conn {
	for i := 0; i < 200; i++ {
		tmp := cli.quicConn
		if tmp != nil {
			return tmp
		}
		time.Sleep(16 * time.Millisecond)
	}
	return nil
}
