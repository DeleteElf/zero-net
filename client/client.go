package client

import "C"
import (
	"context"
	"errors"
	"github.com/DeleteElf/zero-net/framework/network"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/quic-go/quic-go"
	"log/slog"
	"net"
	"time"
)

// Client 客户端
type Client struct {
	Id        string
	SessionId string
	//需要连接的服务端地址
	ServerAddress string
	serverAddr    net.Addr

	Socket        *network.Socket
	StreamConfigs []network.StreamConfig

	OnSocketConnected network.SocketCallbackFunc

	network.Endpoint
}

// NewClient 创建客户端实例
func NewClient(addr string, id string) *Client {
	cli := &Client{
		ServerAddress: addr,
		Id:            id,
	}
	cli.FecBlockSize = network.NetMtuPacketSize
	cli.FecLimitPacketSize = network.FecLimitPacketSize
	cli.SetOnCloseHandler(cli)
	return cli
}

func (cli *Client) CloseChannel(channelId int) bool {
	if !cli.IsClosed() && cli.Socket != nil {
		return cli.Socket.CloseChannel(channelId)
	}
	return false
}
func (cli *Client) OnClosing() bool {
	if cli.Socket != nil {
		slog.Debug("正在关闭客户端的socket！")
		_ = cli.Socket.Close()
		cli.Socket = nil
		slog.Debug("客户端的socket已关闭！")
	}
	if cli.NetConn != nil {
		_ = cli.NetConn.Close()
	}
	return true
}

func (cli *Client) OnClosed() error {
	slog.Debug("客户端已经关闭")
	return nil
}

func (cli *Client) Connect(channelCount int, networkType string, onDisconnect network.SocketCallbackFunc) error {
	if networkType != network.STREAM_NETWORK_UDP {
		return errors.New("暂时只支持udp连接！")
	}
	var err error
	netConn, err := network.NewUdpSocketClient()
	if err != nil {
		slog.Error("创建UDP客户端失败", slog.Any("err", err))
		_ = cli.Close()
		return err
	}
	netAddr, err := net.ResolveUDPAddr(network.STREAM_NETWORK_UDP, cli.ServerAddress)
	if err != nil {
		slog.Error("解析服务端地址失败", slog.Any("err", err))
		return err
	}
	return cli.ConnectToNet(channelCount, netConn, netAddr, onDisconnect)
}
func (cli *Client) ConnectToNet(channelCount int, conn net.PacketConn, addr net.Addr, onDisconnect network.SocketCallbackFunc) error {
	if cli.Socket != nil {
		return errors.New("当前客户端已经连接！")
	}
	if cli.NetConn == nil {
		cli.NetConn = conn
	}
	cli.serverAddr = addr
	err := cli.SetDSCP(conn, 46)
	if err != nil {
		return err
	}
	slog.Debug("正在远程连接", slog.Any("ServerAddress", cli.serverAddr))
	tr := &quic.Transport{
		Conn: cli.NetConn,
	}
	tlsConfig := utils.GenTLSConfig()
	cli.SetDefaultConfig()
	quicConn, err := tr.Dial(context.Background(), cli.serverAddr, tlsConfig, cli.QuicConfig)
	if err != nil {
		slog.Info("远程连接失败！", slog.Any("err", err))
		return err
	}
	if cli.StreamConfigs == nil { //如果没有配置，则默认生成配置
		cli.StreamConfigs = make([]network.StreamConfig, channelCount)
	}

	cli.Socket = network.NewSocket(cli.Id, channelCount, cli.QuicConfig.InitialPacketSize, onDisconnect)
	cli.Socket.FecLimitPacketSize = cli.FecLimitPacketSize
	cli.Socket.StreamConfigs = cli.StreamConfigs
	cli.Socket.CreateChannels()
	cli.Socket.Conn = quicConn
	slog.Info("客户端连接成功！", slog.Int("通道数", cli.Socket.ChannelCount))
	if cli.QuicConfig.EnableDatagrams && cli.SupportFec {
		if cli.Socket.PacketPool == nil {
			cli.Socket.PacketPool = cli.Socket.CreatePacketPool(cli.QuicConfig.InitialPacketSize)
		}
	}
	if cli.OnSocketConnected != nil {
		cli.OnSocketConnected(cli.Socket)
	}
	for i := 0; i < channelCount; i++ {
		err = cli.Socket.InitFecParam(i)
		if err != nil {
			return err
		}

		info := network.StreamInfo{
			Id:           cli.Id,
			ChannelCount: channelCount,
			Ts:           time.Now().Unix(),
			ChannelIndex: i,
			//Type:         int(cli.StreamConfigs[i].Type), //这里需要告诉服务端，是什么类型的流
		}
		stream, err := network.CreateStream(cli.Socket.Conn, info) //创建并打开流
		if err != nil {
			_ = cli.Close()
			return err
		}
		go cli.Socket.HandleChannelStream(i, stream)
	}
	if cli.QuicConfig.EnableDatagrams && cli.SupportFec {
		go cli.Socket.HandleChannelStreamDatagram()
	}
	return nil
}

func (cli *Client) Send(channelId int, data []byte) (bool, error) {

	if cli.IsClosed() {
		return false, errors.New("client is closed")
	}
	if cli.Socket == nil {
		return false, errors.New("socket is null")
	}
	return cli.Socket.Send(channelId, data)

}
