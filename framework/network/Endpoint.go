package network

import (
	"context"
	"errors"
	"fmt"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
	"log/slog"
	"net"
	"time"
)

type Endpoint struct {
	NetConn  net.PacketConn
	QuicConn *quic.Conn

	Config
	IceWorker
	framework.CloseableObject
}

// SetDSCP 为底层 socket 设置 DSCP/ToS 标记。
// tos 为原始 8 位 ToS 字节（高 6 位 DSCP，低 2 位 ECN），例如 46<<2 表示 EF。
// 对 IPv4 设置 IP_TOS，对 IPv6 设置 IPV6_TCLASS；
// 双栈 socket 两个都设，单栈场景其中一个失败属正常，只有两个都失败才返回错误。
// DSCP 值示例：
// 46 (0xB8 >> 2) -> EF (Expedited Forwarding, 极速高优先级，常用于实时音视频/串流)
// 34 (0x88 >> 2) -> AF41 (高优先级数据)
// 注意：SetTOS 传入的是原始 8 位 IPv4 ToS 字节，DSCP 占据高 6 位，因此需要左移 2 位
func (p *Endpoint) SetDSCP(conn net.PacketConn, dscp int) error {
	if conn != nil {
		slog.Debug("正在为连接接设置qos")
		if dscp < 0 || dscp > 63 {
			return fmt.Errorf("setDSCP: DSCP 值 %d 超出 0..63", dscp)
		}
		tos := dscp << 2
		var errs []error
		if err := ipv4.NewPacketConn(conn).SetTOS(tos); err != nil {
			errs = append(errs, fmt.Errorf("警告: 设置 IPv4 DSCP 失败 (可能需要管理员权限或系统支持): %w", err))
		}
		if err := ipv6.NewPacketConn(conn).SetTrafficClass(tos); err != nil {
			errs = append(errs, fmt.Errorf("警告: 设置 IPv6 DSCP 失败 (可能需要管理员权限或系统支持): %w", err))
		}
		if len(errs) == 2 {
			return errors.Join(errs...)
		}
		slog.Debug("设置qos成功！")
	}
	return nil
}

func (p *Endpoint) SetDefaultConfig() {
	if p.QuicConfig == nil {
		p.QuicConfig = &quic.Config{
			// MaxIncomingStreams: 0xffffffffffff, // 最大默认stream输入，默认100
			HandshakeIdleTimeout:    5 * time.Second,  // 默认5s
			MaxIdleTimeout:          10 * time.Second, // 默认30s
			KeepAlivePeriod:         3 * time.Second,  // 建议是 MaxIdleTimeout 的一半，或者更小的值
			InitialPacketSize:       NetMtuPacketSize, //初始包大小
			DisablePathMTUDiscovery: false,            // 允许路径 MTU 探索
			Allow0RTT:               true,
			EnableDatagrams:         p.SupportFec, //允许直接传输udp
			Tracer: func(ctx context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace {
				ctrl := &NetStatusControl{ShowStatusLevel: StatusLevelLostPacket}
				//ctrl.OnCongestionStateChanged = func(tracer *NetStatusTracer) {
				//	//slog.Debug("探测到网络状态发生变化")
				//}
				return NewNetStatusTracer(ctrl)
			},
		}
	}
}
