package client

import (
	"context"
	"errors"
	"github.com/DeleteElf/network-quic/framework"
	"github.com/DeleteElf/network-quic/framework/utils"
	"github.com/DeleteElf/network-quic/streams"
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

// 创建常规的网络接口，这个不对外暴露
func newUdpSocketClient(serverAddr string) (net.PacketConn, net.Addr, error) {
	svrAddr, err := net.ResolveUDPAddr(streams.STREAM_NETWORK_UDP, serverAddr)
	if err != nil {
		return nil, svrAddr, err
	}
	conn, err := net.ListenUDP(streams.STREAM_NETWORK_UDP, nil)
	if err != nil {
		return nil, svrAddr, err
	}
	return conn, svrAddr, nil
}

func (cli *Client) OnClosing() bool {
	if cli.Socket != nil {
		cli.Socket.Close()
		cli.Socket = nil
	}
	if cli.quicConn != nil {
		err := cli.quicConn.CloseWithError(0, "close")
		if err != nil {
			slog.Error("quic close quic connection error")
		}
	}
	if cli.netConn != nil {
		err := cli.netConn.Close()
		if err != nil {
			slog.Error("quic close net connection error")
		}
	}
	return true
}

func (cli *Client) OnClosed() {
	slog.Debug("客户端已经关闭")
}

func (cli *Client) Connect(channelCount int, networkType string) error {
	if cli.Socket != nil {
		return errors.New("当前客户端已经连接！")
	}
	if networkType != "udp" {
		return errors.New("暂时只支持udp连接！")
	}
	cli.Socket = streams.NewSocket(cli.Id, channelCount)
	var err error
	cli.netConn, cli.netAddr, err = newUdpSocketClient(cli.ServerAddress)
	if err != nil {
		slog.Error("客户端连接服务器失败", slog.Any("err", err))
		cli.Close()
		return err
	}
	tlsConfig := utils.GenTLSConfig()
	quicConfig := &quic.Config{
		MaxIncomingStreams:      0xffffffffffff,  // 最大默认stream输入，默认100
		HandshakeIdleTimeout:    5 * time.Second, // 默认5s
		MaxIdleTimeout:          5 * time.Second, // 默认30s
		KeepAlivePeriod:         3 * time.Second, // 建议是 MaxIdleTimeout 的一半，或者更小的值
		InitialPacketSize:       1500,            //当前最大数据包一个基础包的大小
		DisablePathMTUDiscovery: true,
		Allow0RTT:               true,
		// EnableDatagrams:    true,
	}
	slog.Info("quic dial", slog.Any("ServerAddress", cli.netAddr))

	quicConn, err := quic.Dial(context.TODO(), cli.netConn, cli.netAddr, tlsConfig, quicConfig)
	if err != nil {
		slog.Info("quic dial fail", slog.Any("err", err))
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
