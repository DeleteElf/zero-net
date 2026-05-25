package tests

import (
	"fmt"
	"github.com/DeleteElf/network-quic/framework/streams"
	"github.com/DeleteElf/network-quic/framework/utils"
	"github.com/DeleteElf/network-quic/server"
	"log/slog"
	"testing"
	"time"
)

var restart bool = false

func socketHandler(svr *server.Server, clientId string) {
	sock := svr.GetSocket(clientId)
	if sock == nil {
		slog.Error("客户端已经不存在！")
		return
	}
	for i := 0; i < sock.ChannelCount; i++ {
		go messageHandler(svr, clientId, sock, i)
	}
}

func messageHandler(svr *server.Server, clientId string, sock *streams.Socket, channelIndex int) {
	for {
		if svr.IsClosed {
			break
		}
		if sock.IsClosed {
			break
		}
		_, err := sock.ReceiveDataToBuffer(channelIndex) //这个会卡住等待
		if err != nil {
			slog.Error(err.Error())
			break
		}
		if svr.IsClosed {
			break
		}
		if sock.IsClosed {
			break
		}
		currentBuffer := sock.StreamChannels[channelIndex].Buffer
		if currentBuffer == nil {
			break
		}
		sock.StreamChannels[channelIndex].Buffer = nil
		msg := string(currentBuffer.Data)
		slog.Debug("收到数据：", slog.Int("channelId", currentBuffer.ChannelId), slog.String("msg", msg),
			slog.String("clientId", currentBuffer.ClientId))
		if msg == "bye" {
			slog.Debug("收到结束会话命令！")
			_ = svr.CloseSocket(clientId)
		} else if msg == "shutdown" {
			restart = false
			slog.Debug("收到关闭命令！")
			svr.Close()
			return
			//} else if msg == "restart" {
			//	slog.Debug("收到重启命令！")
			//	restart = true
			//	svr.Close()
		} else {
			returnMsg := fmt.Sprintf("收到数据来自[%d]的数据：%s", currentBuffer.ChannelId, msg)
			_, err2 := sock.Send(currentBuffer.ChannelId, []byte(returnMsg))
			if err2 != nil {
				slog.Error(err2.Error())
			}
		}
	}
}

func TestServer(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)               //初始化日志
	svr := server.NewServerByAddress("0.0.0.0:10001") //尝试连接本机服务
	//svr := server.NewServerByPort(10001, false) //尝试连接本机服务
	for {
		if restart {
			time.Sleep(1 * time.Second)
			slog.Debug("服务端重新启动监听！")
			restart = false
		}
		svr.OnAcceptSocket = func(id string) {
			slog.Debug("新的客户端接入：", slog.String("id", id))
			go socketHandler(svr, id)
		}
		svr.StartListen(func(id string) {
			slog.Debug("客户端断开连接：", slog.String("id", id))
		})
		slog.Debug("服务端退出监听！")
		if !restart {
			break
		}
	}
}
