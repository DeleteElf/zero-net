package stun

import (
	"github.com/DeleteElf/network-quic/framework"
	"github.com/pion/stun/v3"
	"log/slog"
)

type Client struct {
	ExternalAddress stun.XORMappedAddress
	framework.CloseableObject
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) Connect(address, token string) error {
	//address stun:stun.l.google.com:19302
	// 1. 创建指向公共 STUN 服务器的 UDP 连接 (这里以谷歌公共服务器为例)
	uri, err := stun.ParseURI(address)
	if err != nil {
		//log.Fatalf("解析 STUN URI 失败: %v", err)
		return err
	}
	cli, err := stun.DialURI(uri, &stun.DialConfig{})
	if err != nil {
		//log.Fatalf("连接 STUN 服务器失败: %v", err)
		return err
	}
	defer func(c *stun.Client) {
		_ = cli.Close()
	}(cli)

	// 2. 构建一个绑定请求 (Binding Request)
	message := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	if len(token) > 0 {
		message.Add(stun.AttrUsername, []byte(token))
	}
	// 3. 发送请求并监听 STUN 服务器的响应
	if err := cli.Do(message, func(res stun.Event) {
		if res.Error != nil {
			slog.Error("STUN 响应错误: ", slog.Any("err", res.Error))
			return
		}
		if err := c.ExternalAddress.GetFrom(res.Message); err != nil {
			slog.Error("解析 XOR-MAPPED-ADDRESS 失败:  ", slog.Any("err", err))
			return
		}
	}); err != nil {
		return err
	}
	return nil
}

func (c *Client) OnClosing() bool {
	slog.Debug("正在断开stun连接...")
	return true
}

func (c *Client) OnClosed() {
	slog.Debug("stun已经断开！")
}
