package tests

import (
	"fmt"
	"github.com/DeleteElf/network-quic/server"
	"github.com/DeleteElf/network-quic/utils"
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
	for i := 0; i < sock.Count; i++ {
		go messageHandler(svr, clientId, sock, i)
	}
}

func messageHandler(svr *server.Server, clientId string, sock *server.Socket, channelIndex int) {
	for {
		if svr.IsClosed {
			break
		}
		err := sock.ReceiveDataToBuffer(channelIndex) //这个会卡住等待
		if !err {
			slog.Error("从缓存中获取接收数据失败！")
			break
		}
		currentBuffer := sock.CurrentBuffers[channelIndex]
		if currentBuffer == nil {
			break
		}
		sock.CurrentBuffers[channelIndex] = nil
		msg := string(currentBuffer.Data)
		slog.Debug("收到数据：", slog.Int("channelId", currentBuffer.ChannelId), slog.String("msg", msg))
		if msg == "bye" {
			slog.Debug("收到结束会话命令！")
			_ = svr.CloseSocket(clientId)
		} else if msg == "shutdown" {
			restart = false
			slog.Debug("收到关闭命令！")
			svr.Close()
			//} else if msg == "restart" {
			//	slog.Debug("收到重启命令！")
			//	restart = true
			//	svr.Close()
		} else {
			returnMsg := fmt.Sprintf("收到数据来自[%d]的数据：%s", currentBuffer.ChannelId, msg)
			_, err2 := sock.Send(sock.Streams[currentBuffer.ChannelId], []byte(returnMsg))
			if err2 != nil {
				slog.Error(err2.Error())
			}
		}
	}
}

func TestServer(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)             //初始化日志
	svr := server.NewServer("0.0.0.0:10001", false) //尝试连接本机服务
	//svr := server.NewServerByPort(10001, false) //尝试连接本机服务
	for {
		if restart {
			time.Sleep(1 * time.Second)
			slog.Debug("服务端重新启动监听！")
			restart = false
		}
		go svr.StartListen()
		for {
			select {
			case id := <-svr.OnAccept:
				if len(id) == 0 {
					break
				}
				slog.Debug("新的客户端接入：", slog.String("id", id))
				go socketHandler(svr, id)
			}
			if svr.IsClosed {
				break
			}
		}
		slog.Debug("服务端退出监听！")
		if !restart {
			break
		}
	}
}
