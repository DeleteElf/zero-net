package network

import (
	"github.com/quic-go/quic-go"
	"time"
)

const (
	FecPacketHeaderLength = 26
	// FecLimitPacketSize 我们传输音频包 48000质量的 opus 最小是60+12的rtp数据包，因此默认限制一下70
	FecLimitPacketSize       = 70
	NetMtuPacketSize         = 1400
	VideoHeaderLength        = 32
	AudioHeaderLength        = 24
	RtpHeaderLength          = 12
	NvidiaPacketHeaderLength = 16
	// RtpHeader 普通rtp标准头
	RtpHeader = 0x80
	// RtpHeaderFlagExtension rtp头携带了扩展的信息
	RtpHeaderFlagExtension = 0x10
	// VideoHeader rtp头携带扩展信息后的头信息
	VideoHeader        = RtpHeader | RtpHeaderFlagExtension
	AudioHeader        = 97
	AudioDynamicHeader = 127

	CustomFecHeader   = 0x81
	CustomMessageType = 0x11

	//MessageExpireTime  = 200 * time.Millisecond
	//AudioExpireTime    = 100 * time.Millisecond
	//VideoExpireTime    = 100 * time.Millisecond
	AudioOosExpireTime = 50 * time.Millisecond
	//VideoOosExpireTime = 500 * time.Millisecond
	// AudioDataTime 音频数据包的间隔时长
	AudioDataTime = 10 * time.Millisecond

	//SPECULATIVE_RFI_COOLDOWN_PERIOD_MS = 300000
	SPECULATIVE_RFI_COOLDOWN_PERIOD_US = 300000000
)

type FecLevel int

const (
	// FecDisabled 关闭Fec编解码支持
	FecDisabled FecLevel = iota
	// FecDepacketizeKeepRtpPacketAndSize 保留RtpPacket结构和大小，仅执行fec修复
	FecDepacketizeKeepRtpPacketAndSize
	// FecDepacketizeKeepRtpPacket 最佳方案。保留RtpPacket结构，除了执行fec修复外，会执行解包，将整个block分块数据拼接成大数据包,这个模式会修改rtp的数据格式，如果是音频的话，空包也不会保留大小
	FecDepacketizeKeepRtpPacket
	// FecDepacketizeKeepRtpData 最高性能，因为直接使用分片数据即可，去掉Rtp数据包结构，仅保留拼接好的视频编码数据,与 reedsolomon 配合性能最佳，不需要恢复rtp结构
	FecDepacketizeKeepRtpData
)

type StreamConfig struct {
	//当前流的数据业务类型
	Type StreamType
	//是否启用Fec功能
	FecEnableLevel FecLevel
	//数据分片，仅当上层不是标准rtp数据包时，因没有提供fecInfo才用得到，否则优先考虑rtp包内协议
	DataShards uint8
	//奇偶碎片，仅当上层不是标准rtp数据包时，因没有提供fecInfo才用得到，否则优先考虑rtp包内协议
	ParityShards uint8

	FecPacketSize uint16
}

type Config struct {
	SupportFec bool
	//fec数据包的分块大小，数据包数据大小，不能大于这个数值
	FecBlockSize uint16
	//fec数据包的比例，如果包大小不够，则会是0
	//FecPercentage int
	//fec数据包的最小大小，小于这个值就不再执行fec
	FecLimitPacketSize int
	//如果执行fec，fec在总数量中的最小数量，默认是2，当FecPercentage==0时，不校验此参数
	FecMinRequiredPackets int
	//quic的配置
	QuicConfig *quic.Config
}

func (c *StreamConfig) SetStreamType(t StreamType) {
	if c.Type != t {
		c.Type = t
		switch c.Type {
		case Audio: //音频，默认50%
			c.FecEnableLevel = FecDepacketizeKeepRtpPacket
			c.DataShards = 4
			c.ParityShards = 2
		case Video: //视频，默认30%
			c.FecEnableLevel = FecDepacketizeKeepRtpPacket
			c.DataShards = 10
			c.ParityShards = 3
			break
		default:
			c.FecEnableLevel = FecDisabled
			break
		}
	}
}
