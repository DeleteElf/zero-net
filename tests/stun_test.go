package tests

import (
	"github.com/DeleteElf/network-quic/framework/utils"
	"github.com/DeleteElf/network-quic/stun"
	"log/slog"
	"testing"
)

func TestStunClient(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)
	client := stun.NewStunClient()
	err := client.Connect("stun:127.0.0.1:3478")
	if err != nil {
		slog.Error("连接stun服务时发生错误！", slog.Any("err", err))
	} else {
		slog.Info("====================================")
		slog.Info("你的公网 IP 地址 :", slog.Any("ip", client.ExternalAddress.IP))
		slog.Info("你的公网映射端口 : ", slog.Any("port", client.ExternalAddress.Port))
		slog.Info("====================================")
	}

}
