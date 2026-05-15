package tests

import (
	"errors"
	"fmt"
	"github.com/DeleteElf/network-quic/server"
	"github.com/DeleteElf/network-quic/utils"
	"log/slog"
	"testing"
)

func messageHandler(svr *server.Server) error {
	err := svr.ReceiveDataToBuffer(0) //这个会卡住等待
	if !err {
		return errors.New("从缓存中获取接收数据失败！")
	}
	currentBuffer := svr.CurrentBuffers[0]
	svr.CurrentBuffers[0] = nil
	msg := string(currentBuffer.Data)
	slog.Debug("收到数据：", slog.Int("channelId", currentBuffer.ChannelId), slog.String("msg", msg))
	if msg == "bye" {
		svr.Close()
	} else {
		returnMsg := fmt.Sprintf("收到数据来自[%d]的数据：%s", currentBuffer.ChannelId, msg)
		_, err2 := svr.Send(svr.Streams[currentBuffer.ChannelId], []byte(returnMsg))
		if err2 != nil {
			return err2
		}
	}
	return nil
}

func TestServer(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)             //初始化日志
	svr := server.NewServer("0.0.0.0:10001", false) //尝试连接本机服务
	//svr := server.NewServerByPort(10001, false) //尝试连接本机服务
	go func() {
		for {
			if svr.IsClosed {
				break
			}
			err := messageHandler(svr)
			if err != nil {
				break
			}
		}
	}()
	svr.StartListen()
}
