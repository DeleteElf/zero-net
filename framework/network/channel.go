package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/klauspost/reedsolomon"
	"github.com/quic-go/quic-go"
	"io"
	"log/slog"
	"sync"
	"time"
)

type StreamChannelOperating interface {
	CreateChannels(count int)
	HandleChannelStreamData(channel chan StreamChannelData, channelId int, stream *quic.Stream)
	Send(channelId int, data []byte) (bool, error)
}

// StreamChannelData 流通道数据结构
type StreamChannelData struct {
	ClientId  string
	ChannelId int
	Offset    int
	Data      []byte
}

type MessageChannelCallbackFunc func(string, int)
type FrameLostFunc func(uint8, uint32, uint32)

type StreamChannel struct {
	Channel       chan StreamChannelData
	ClientId      string
	ChannelId     int
	Cancel        context.CancelFunc
	Done          bool
	Buffer        *StreamChannelData
	Stream        *quic.Stream
	FecEncoders   map[string]reedsolomon.Encoder
	Depacketizers map[uint8]*Depacketizer
	Packetizers   map[uint8]*Packetizer
	lockEncoders  sync.Mutex
	lockFecGroups sync.Mutex

	OnConnect    MessageChannelCallbackFunc
	OnDisconnect MessageChannelCallbackFunc
	OnFrameLost  FrameLostFunc
	framework.CloseableObject
}

// NewStreamChannel 创新数据通道，并确定传输类型
//
//	-param id:数据通道的编号
//	-param index:数据通道的索引
//	-param c:数据通道的配置
//
// return:通道实例
func NewStreamChannel(id string, index int) *StreamChannel {
	slog.Debug("正在创建通道", slog.String("id", id), slog.Int("ChannelId", index))
	//cacheCount := 2
	//if index >= 2 {
	//	cacheCount = 60
	//}
	sc := &StreamChannel{
		Channel:       make(chan StreamChannelData, 200),
		ClientId:      id,
		ChannelId:     index,
		Depacketizers: make(map[uint8]*Depacketizer), //初始化空的解包器
		Packetizers:   make(map[uint8]*Packetizer),   //初始化空的打包器
		FecEncoders:   make(map[string]reedsolomon.Encoder),
		CloseableObject: framework.CloseableObject{
			IsClosed: false,
		},
	}
	sc.Depacketizers[0] = NewDepacketizer() //初始化一个组
	sc.SetOnCloseHandler(sc)
	return sc
}

func (sc *StreamChannel) OnClosing() bool {
	if sc.Cancel != nil {
		sc.Cancel()
	}
	sc.Cancel = nil
	if sc.Stream != nil {
		sc.Stream.CancelRead(0)
		_ = sc.Stream.Close()
		sc.Stream.CancelWrite(0)
		sc.Stream = nil
	}
	count := 100
	for i := 0; i < count; i++ {
		if !sc.Done {
			time.Sleep(time.Millisecond)
			continue
		}
		break
	}
	return true
}

func (sc *StreamChannel) OnClosed() error {
	slog.Debug("检测到通道已经退出！", slog.String("id", sc.ClientId), slog.Int("通道", sc.ChannelId))
	sc.Buffer = nil
	return nil
}

// HandleChannelStreamData 从通道接收流的数据
func (sc *StreamChannel) HandleChannelStreamData(stream *quic.Stream) {
	sc.Stream = stream
	_, sc.Cancel = context.WithCancel(sc.Stream.Context())
	defer func() {
		if !sc.Done && sc.Channel != nil {
			close(sc.Channel)
			sc.Channel = nil
		}
		sc.Done = true
		if sc.OnDisconnect != nil {
			sc.OnDisconnect(sc.ClientId, sc.ChannelId)
		}
	}()
	slog.Debug("完成流与通道的对接，开始读取通道数据", slog.Int("channel", sc.ChannelId))
	if sc.OnConnect != nil {
		sc.OnConnect(sc.ClientId, sc.ChannelId)
	}
	for {
		if sc.IsClosed {
			return
		}
		buf, err := utils.ReadStreamByHeaderUShort(sc.Stream)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) { //如果是读取超时，我们就继续即可
				continue
			} else if err != io.EOF {
				//信息太频繁，不用一直提示
				//slog.Error("通道读取失败！", slog.Int("ChannelId", sc.ChannelId), slog.Any("err", err))
			} else {
				//slog.Error("通道流已经结束！", slog.Int("ChannelId", sc.ChannelId))
			}
			return
		}
		if sc.IsClosed {
			return
		}
		if len(buf) == 0 { //读取到0长度的数据包，我们认为是断开连接了
			return
		}
		if sc == nil {
			return
		}
		if sc.Channel == nil {
			return
		}
		sc.Channel <- StreamChannelData{
			ClientId:  sc.ClientId,
			ChannelId: sc.ChannelId,
			Offset:    0,
			Data:      buf,
		}
	}
}

func (sc *StreamChannel) ReceiveDataToBuffer() bool {
	if sc.Buffer == nil { //当前缓存没有工作时
		buffer, ok := <-sc.Channel
		if !ok {
			//slog.Warn("通道已经关闭！")
			return ok
		}
		sc.Buffer = &buffer
	}
	return true
}

func (sc *StreamChannel) Send(data []byte) (bool, error) {
	if sc.IsClosed {
		return false, nil
	}
	if sc.Stream == nil {
		return false, nil
	}
	err := utils.WriteStreamByHeaderUShort(sc.Stream, data)
	return err == nil, err
}

func (sc *StreamChannel) CheckDataReceiveTimeout(group *FecGroup, groups *Depacketizer) bool {
	if group.ExpiredAt.Before(time.Now()) { //如果已经过期，则不再等待，直接接收下一个
		if group.HeaderTemplate[0] == RtpHeader && //音频数据是陆续发送的，我们允许按时间递增等待
			(group.HeaderTemplate[1] == AudioHeader || group.HeaderTemplate[1] == AudioDynamicHeader) {
			//expireTime := group.OosTime.Add(AudioOosExpireTime + time.Duration(group.ShardCount-1)*AudioDataTime)
			//if expireTime.Before(time.Now()) {
			//	slog.Debug("音频帧，oos检测，数据接收超时丢弃！", slog.Int("channel", sc.ChannelId),
			//		slog.Any("groupId", groups.CurrentGroupId), slog.Any("已接收", group.Received),
			//		slog.Any("合计", len(group.Shards)))
			//todo:这里需要补充一下数据并告诉上层逻辑
			for i := 0; i < int(group.ShardCount); i++ {
				if sc.Channel != nil {
					if group.Shards[i] == nil { //没有到的数据，补充一个空数据进去
						group.Shards[i] = make([]byte, group.ShardDataLength) //音频数据不用重发，直接填满空洞即可
					}
					sc.Channel <- StreamChannelData{
						ClientId:  sc.ClientId,
						ChannelId: sc.ChannelId,
						Offset:    0,
						Data:      group.Shards[i], //直接使用原始数据包，实现零拷贝
					}
				}
			}
			//	delete(groups.Groups, groups.CurrentGroupId)
			//	groups.CurrentGroupId++ // 单协程处理下无需 atomic，若多协程则整体加锁
			//	return true
			//}
			//return false
		} else if group.HeaderTemplate[0] == VideoHeader { //如果是我们的视频包
			//expireTime := group.OosTime.Add(VideoOosExpireTime)
			//if expireTime.Before(time.Now()) {
			//	slog.Debug("视频帧，oos检测，数据接收超时丢弃！", slog.Int("channel", sc.ChannelId),
			//		slog.Any("groupId", groups.CurrentGroupId), slog.Any("已接收", group.Received),
			//		slog.Any("合计", len(group.Shards)))
			//	delete(groups.Groups, groups.CurrentGroupId)
			//	groups.CurrentGroupId++ // 单协程处理下无需 atomic，若多协程则整体加锁
			//	return true
			//}
			return false //如果没有超过
		}
		slog.Debug("帧接收超时丢弃！", slog.Int("channel", sc.ChannelId),
			slog.Any("groupId", groups.CurrentGroupId), slog.Any("已接收", group.Received),
			slog.Any("合计", len(group.Shards)))
		delete(groups.Groups, groups.CurrentGroupId)
		groups.CurrentGroupId++ // 单协程处理下无需 atomic，若多协程则整体加锁
		//continue        //过期了，不论是否是关键帧，我们都丢弃了，那么还需要继续等待下一个
		return true
	}
	return false
}

func (sc *StreamChannel) RtpAddPacket(d *Depacketizer, packet *FecPacket) *FecGroup {
	group, exists := d.Groups[packet.Header.GroupIdx]
	isRtp := packet.Payload[0] == RtpHeader || packet.Payload[0] == VideoHeader
	if !exists {
		expireTime := MessageExpireTime
		if isRtp {
			expireTime = AudioExpireTime
			if packet.Payload[0] == VideoHeader {
				expireTime = VideoExpireTime
			}
		}
		totalShards := packet.Header.DataShards + packet.Header.ParityShards
		group = &FecGroup{
			HeaderSample: &packet.Header, ExpiredAt: time.Now().Add(expireTime),
			Shards: make([][]byte, totalShards), Packets: make([]*FecPacket, totalShards),
			OosTime: time.Now(), ShardCount: packet.Header.DataShards, ShardDataLength: packet.Header.Length,
			//GroupID: packet.Header.GroupIdx, DataShards: packet.Header.DataShards, ParityShards: packet.Header.ParityShards,
			//Total: packet.Header.Total, Received: 0, CreatedAt: time.Now(),
		}
		if isRtp { //如果判定是rtp包，我们就需要预处理一下数据，方便后期补充rtp包
			switch packet.Payload[1] {
			case 0x61: //标准音频
				group.HeaderTemplate = packet.Payload[:RtpHeaderLength]
			case 0x7f: //动态音频
				group.HeaderTemplate = packet.Payload[:AudioHeaderLength]
			default: //其他都是视频 视频数据的rtp包数据都是一样的
				group.HeaderTemplate = packet.Payload[:VideoHeaderLength]
			}
		} else {
			group.HeaderTemplate = packet.Payload[:FecPacketHeaderLength] //因为信息一致性，我们其实不用再次赋值
		}
		sc.Depacketizers[packet.Header.Ssrc].Groups[packet.Header.GroupIdx] = group
	}
	outOfSequence := false
	if packet.Header.SequenceNumber != d.NextSequenceNumber {
		//slog.Debug("收到无序帧", slog.Int("channel", sc.ChannelId), slog.Any("group", packet.Header.GroupIdx),
		//	slog.Any("SequenceNumber", packet.Header.SequenceNumber))
		if utils.IsBefore16(packet.Header.SequenceNumber, d.NextSequenceNumber) {
			// 迟到/重复包：检查是否已存在
			if group.Packets[packet.Header.ShardIdx] != nil {
				return group
			}
			outOfSequence = true
		} else {
			// 收到比预期大的 Seq，说明中间发生了缺包，累加 MissingPackets
			d.MissingPackets += packet.Header.SequenceNumber - d.NextSequenceNumber
			d.NextSequenceNumber = packet.Header.SequenceNumber + 1 //ps：这里直接跳到了最终
		}
	} else {
		d.NextSequenceNumber++
	}

	if group.Packets[packet.Header.ShardIdx] == nil { //不接收一样的数据包,通过Packet来判断，shards因为需要用于恢复，这里不进行判断
		headerLength := FecPacketHeaderLength
		if isRtp { //如果是rtp包
			switch packet.Payload[1] { //packetType
			case 97:
				headerLength = RtpHeaderLength
			case 127:
				headerLength = AudioHeaderLength
			default: //video
				headerLength = VideoHeaderLength
			}
		}
		if int(packet.Header.Length) != len(packet.Payload[headerLength:]) { //数据包载体长度不一致，则丢弃
			slog.Debug("数据长度不一致，丢弃！", slog.Int("channel", sc.ChannelId),
				slog.Any("groupId", packet.Header.GroupIdx),
				slog.Any("shardIndex", packet.Header.ShardIdx),
				slog.Any("targetLength", packet.Header.Length),
				slog.Int("shardDataLength", len(packet.Payload[headerLength:])))
			return group
		}
		group.Shards[packet.Header.ShardIdx] = packet.Payload[headerLength:] //只加入验证过的数据
		group.Packets[packet.Header.ShardIdx] = packet                       // 记录原始包指针
		if outOfSequence && d.MissingPackets > 0 {                           //若这是补入的乱序包，修正 MissingPacket
			d.MissingPackets--
		}
		if !group.HasParityShard && packet.Header.ShardIdx >= packet.Header.DataShards {
			group.HasParityShard = true
		}
		group.Received++
	}
	presentationTimeUs := uint64(packet.Header.Timestamp) * 1000 / 90
	if outOfSequence { //无序状态的数据，我们记录一下
		d.LastOosFramePresentationTimestamp = presentationTimeUs
		if !d.ReceivedOosData { //接收到无序的数据了
			d.ReceivedOosData = true
		}
	} else if d.ReceivedOosData && presentationTimeUs > d.LastOosFramePresentationTimestamp+SPECULATIVE_RFI_COOLDOWN_PERIOD_US { //从无序中恢复
		d.ReceivedOosData = false
	}
	return group
}

func (sc *StreamChannel) notifyFrameLost(ssrc uint8, frameIndex uint32, speculative bool) {
	//todo:notifyFrameLost(trackIndex,queue->currentFrameNumber, true);
	depacketizer := sc.Depacketizers[ssrc]
	if !depacketizer.WaitingForIdrFrame {
		//LC_ASSERT(depacketizer->waitingForRefInvalFrame);
		message := "发送针对不可恢复帧的RFI请求"
		if speculative {
			message = "针对预测的帧丢失发送推测性信息请求（RFI）"
		}
		slog.Debug(message, slog.Any("ssrc", ssrc), slog.Any("frameIndex", frameIndex))
		startFrameIndex := depacketizer.CurrentFrameIndex
		depacketizer.CurrentFrameIndex = frameIndex + 1 //通知服务器当前帧丢失后，当前帧+1
		if sc.OnFrameLost != nil {                      //方案1：回调
			sc.OnFrameLost(ssrc, startFrameIndex, frameIndex)
		} else { //方案2：自己发
			sc.connectionDetectedFrameLoss(ssrc, startFrameIndex, frameIndex)
		}
	}
}

func (sc *StreamChannel) connectionDetectedFrameLoss(ssrc uint8, start, end uint32) {
	sc.requestRfiFrame(ssrc, start, end)
	////todo:这个暂时先有上层逻辑显示,存在lbq的逻辑
	//if !utils.IsBefore32(end, start) { //start<=end
	//	data := make([]byte, 13)                        //如果加上头 应该是15
	//	binary.LittleEndian.PutUint16(data[0:], 0x0350) //这里还不是非常确定是这个，因为主机端没有看到对应的接收
	//	binary.LittleEndian.PutUint32(data[2:], uint32(ssrc))
	//	binary.LittleEndian.PutUint32(data[6:], start)
	//	binary.LittleEndian.PutUint32(data[10:], end)
	//	data[14] = 0x01 //Rfi
	//	//_, _ = sc.Send(data)
	//	slog.Debug("发送Rfi帧申请！协议不太对", slog.Any("ssrc", ssrc), slog.Any("startFrameIndex", start), slog.Any("endFrameIndex", end))
	//}
}

// 用来通知主机，已经完成的帧，当我们接收到一个帧时，更新当前帧的编号，如果该帧是LTR（丢失传输请求），则发送ACK（确认）控制消息
func (sc *StreamChannel) connectionReceivedCompleteFrame(ssrc uint8, start uint32, frameIsLTR bool) {
	//setLastGoodFrame(trackIndex,frameIndex);
	if frameIsLTR { //LTR_ACK 告诉主机，此帧可以参考
		//todo:这个暂时先有上层逻辑显示,存在lbq的逻辑
		data := make([]byte, 15)                        //如果加上头 应该是15
		binary.LittleEndian.PutUint16(data[0:], 0x0350) //SS_LTR_FRAME_ACK_PTYPE
		binary.LittleEndian.PutUint32(data[2:], uint32(ssrc))
		binary.LittleEndian.PutUint32(data[6:], start)
		//binary.LittleEndian.PutUint32(data[10:], 0)
		//data[14] = 0x00 //Rfi
		_, _ = sc.Send(data)
		slog.Debug("发送LTR_ACK", slog.Any("ssrc", ssrc), slog.Any("startFrameIndex", start))
	}
}

// 请求rfi参考帧
func (sc *StreamChannel) requestRfiFrame(ssrc uint8, start, end uint32) {
	if !utils.IsBefore32(end, start) { //start<=end
		data := make([]byte, 26)
		binary.LittleEndian.PutUint16(data[0:], 0x0301) //IDX_INVALIDATE_REF_FRAMES
		if start < 0x20 {
			binary.LittleEndian.PutUint32(data[2:], 0)
			binary.LittleEndian.PutUint32(data[10:], start)
		} else {
			binary.LittleEndian.PutUint32(data[2:], start)
			binary.LittleEndian.PutUint32(data[10:], end)
		}
		binary.LittleEndian.PutUint32(data[18:], uint32(ssrc))
		_, _ = sc.Send(data)
		slog.Debug("发送Rfi帧申请成功", slog.Any("ssrc", ssrc), slog.Any("startFrameIndex", start), slog.Any("endFrameIndex", end))
	} else {
		slog.Debug("发送Rfi帧申请失败，start大于end", slog.Any("ssrc", ssrc), slog.Any("startFrameIndex", start), slog.Any("endFrameIndex", end))
	}
}

// 请求关键帧
func (sc *StreamChannel) requestIdrFrame(ssrc uint8) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint16(data[0:], 0x0302)       //报头 IDX_REQUEST_IDR_FRAME
	binary.LittleEndian.PutUint16(data[2:], uint16(ssrc)) //start
	_, _ = sc.Send(data)
	slog.Debug("发送Idr 关键帧请求", slog.Any("ssrc", ssrc))
}

func (sc *StreamChannel) FecDecode(packet *FecPacket) error {
	//slog.Debug("fec开始解码", slog.Any("channel id", sc.ChannelId), slog.Any("ssrc", packet.Header.Ssrc), slog.Any("groupId", packet.Header.GroupIdx))
	totalShards := packet.Header.DataShards + packet.Header.ParityShards
	if packet.Header.ShardIdx >= totalShards {
		return fmt.Errorf("无效的shard索引: %d", packet.Header.ShardIdx)
	}
	depacketizer, exists := sc.Depacketizers[packet.Header.Ssrc]
	if !exists {
		return fmt.Errorf("无效的通道数据: %d", packet.Header.Ssrc)
	}
	sc.RtpAddPacket(depacketizer, packet)
	for {
		next, exists := depacketizer.Groups[depacketizer.CurrentGroupId]
		if !exists {
			if packet.Header.Header == 0x80 {
				if utils.IsBefore8(depacketizer.CurrentGroupId, packet.Header.GroupIdx) { //只需要处理数据包比当前待处理的还新，这一个问题
					slog.Debug("收到了新的音频，但是不是期望的音频", slog.Any("target", depacketizer.CurrentGroupId),
						slog.Any("received", packet.Header.GroupIdx))
					tempGroup := depacketizer.Groups[packet.Header.GroupIdx]
					if tempGroup.Received >= tempGroup.ShardCount { //新的已经接收满了
						//todo:这里可能需要补充空洞数据,如果不补，应该也可以，从时间维度来说，无形中，还能追帧，从效果来说，可能出现音爆
						//target := uint16(packet.Header.GroupIdx)
						//if packet.Header.GroupIdx < depacketizer.CurrentGroupId {
						//	target += math.MaxUint8
						//}
						//for i := uint16(depacketizer.CurrentGroupId); i < target; i++ {
						//	group, exists := depacketizer.Groups[uint8(i)]
						//	if exists {
						//		for i := 0; i < int(group.ShardCount); i++ {
						//			if sc.Channel != nil {
						//				if group.Shards[i] == nil { //没有到的数据，补充一个空数据进去
						//					group.Shards[i] = make([]byte, group.ShardDataLength) //音频数据不用重发，直接填满空洞即可
						//				}
						//				sc.Channel <- StreamChannelData{
						//					ClientId:  sc.ClientId,
						//					ChannelId: sc.ChannelId,
						//					Offset:    0,
						//					Data:      group.Shards[i], //直接使用原始数据包，实现零拷贝
						//				}
						//			}
						//		}
						//	} else {
						//		continue
						//	}
						//}
						slog.Debug("新的音频数据已经满足解码，跳到最新音频数据", slog.Any("groupId", packet.Header.GroupIdx))
						depacketizer.CurrentGroupId = packet.Header.GroupIdx //直接跳到当前，旧的全部舍弃
						continue
					}
				}
			}
			break // 下一个组还没到来，退出循环
		}
		// 如果当前等待的组包数量还不足以解码，直接中断等待下一个网络包到达，切勿死循环！
		header := next.HeaderSample //连续组装，不能使用packet，而应该从当前分组取样本
		if next.Received < header.DataShards {
			if packet.Header.Header == 0x80 { //声音做简单的超时即可
				if sc.CheckDataReceiveTimeout(next, depacketizer) {
					continue
				}
			} else { //视频
				if !depacketizer.ReportedLostFrame && !depacketizer.ReceivedOosData {
					if depacketizer.MissingPackets > uint16(header.ParityShards) {
						sc.notifyFrameLost(packet.Header.Ssrc, depacketizer.CurrentFrameIndex, true)
						depacketizer.ReportedLostFrame = true
					}
				}
			}
			break // 无论音频还是视频，数据未凑齐前退出循环，等待下一个 UDP 包
		}
		//事实证明数据凑齐了：检查是否打脸了之前的 Speculative RFI 误报
		if depacketizer.ReportedLostFrame && !depacketizer.ReceivedOosData {
			// 如果事实证明我们对主办方撒了谎，那就暂时停止进一步的推测性信息请求（RFI）。
			depacketizer.ReceivedOosData = true
			depacketizer.LastOosFramePresentationTimestamp = uint64(header.Timestamp) * 1000 / 90
			slog.Debug("因误判丢帧退出 Speculative RFI 模式", slog.Any("frame", depacketizer.CurrentFrameIndex))
		}

		if next.HasParityShard { //包含奇偶校验分片，才执行
			for i := 0; i < int(header.DataShards); i++ {
				if next.Shards[i] == nil { // 丢失的分片：分配 maxLen 字节的零值切片供 RS 恢复
					next.Shards[i] = make([]byte, next.ShardDataLength)
				}
				if len(next.Shards[i]) != int(next.ShardDataLength) {
					slog.Debug("数据分片长度错误！", slog.Int("shardIndex", i),
						slog.Any("targetLength", next.ShardDataLength),
						slog.Int("shardDataLength", len(next.Shards[i])))
				}
			}
			// 关键优化：使用 ReconstructData 仅恢复数据分片，比 Reconstruct 省时省 CPU
			encoder, err := sc.GetFecEncoder(header.DataShards, header.ParityShards)
			if err != nil {
				return fmt.Errorf("获取fec解码器出错【%d】: %w", header.GroupIdx, err)
			}
			err = encoder.ReconstructData(next.Shards)
			if err != nil {
				for i := 0; i < int(totalShards); i++ {
					slog.Debug("解码报错，输出实际数据验证", slog.Int("shardIndex", i),
						slog.Any("targetLength", next.ShardDataLength),
						slog.Int("shardDataLength", len(next.Shards[i])))
				}
				delete(depacketizer.Groups, depacketizer.CurrentGroupId)
				depacketizer.CurrentGroupId++
				return fmt.Errorf("fec解码出错【%d】:  %w", header.GroupIdx, err)
			}
			//slog.Debug("执行Fec解码逻辑", slog.Any("groupId", header.GroupIdx))
		}
		//slog.Debug("解码fec完成", slog.Int("groupId", int(next.GroupID)))
		delete(depacketizer.Groups, header.GroupIdx)
		depacketizer.CurrentGroupId++ // 单协程处理下无需 atomic，若多协程则整体加锁
		depacketizer.CurrentBlockIndex++
		if depacketizer.CurrentBlockIndex >= next.HeaderSample.BlockCount { //执行下一帧
			depacketizer.CurrentBlockIndex = 0
			depacketizer.CurrentFrameIndex++
			depacketizer.ReportedLostFrame = false
		}

		if next.HeaderTemplate[0] == RtpHeader || next.HeaderTemplate[0] == VideoHeader { //如果是rtp包
			//这里可以根据特性进行拼接数据,如果考虑尽量零拷贝处理next.Shards
			//现在这里有几个问题：
			//1，我需要补充没有到的正规rtp 包的头信息 ，假设 一共6个数据包，数据分片是4个，当前到达的索引是 0,1,3,4，那么则需要补充序号是2的rtp头
			//2，我需要告诉上层逻辑，fec已经处理完毕了
			isVideo := next.HeaderTemplate[1] != 0x61 && next.HeaderTemplate[1] != 0x7f
			//slog.Debug("fec开始重组", slog.Any("channel id", sc.ChannelId), slog.Any("groupId", header.GroupIdx))
			for i := 0; i < int(header.DataShards); i++ {
				if !isVideo { //先只修改音频支持
					if sc.Channel != nil {
						//slog.Debug("执行音频直接接收并解码", slog.Int("size", len(next.Shards[i])))
						sc.Channel <- StreamChannelData{
							ClientId:  sc.ClientId,
							ChannelId: sc.ChannelId,
							Offset:    0,
							Data:      next.Shards[i], //直接使用原始数据包，实现零拷贝
						}
					}
				} else {
					var resultData []byte
					if next.Packets[i] == nil { //Payload 是携带rtp包头信息的完整数据缓存
						resultData = RebuildRtpPacket(next.HeaderTemplate, next.Shards[i], uint8(i), header.DataShards)
					} else { //清除fecPercentage的数据
						resultData = next.Packets[i].Payload //直接使用原始数据包，实现零拷贝
					}
					if isVideo { //因为我们已经处理过fec了，必须告诉上层没有fec分片数据了
						oldFecInfo := binary.LittleEndian.Uint32(resultData[28:])
						binary.LittleEndian.PutUint32(resultData[28:32], oldFecInfo&^(0x7F<<4))
					}
					//slog.Debug("fec重组了一条数据", slog.Any("channel id", sc.ChannelId), slog.Any("shard index", i))
					if sc.Channel != nil {
						sc.Channel <- StreamChannelData{
							ClientId:  sc.ClientId,
							ChannelId: sc.ChannelId,
							Offset:    0,
							Data:      resultData, //直接使用原始数据包，实现零拷贝
						}
					}
				}
			}
			//slog.Debug("fec完成重组", slog.Any("channel id", sc.ChannelId), slog.Any("groupId", header.GroupIdx))
		} else {
			//slog.Debug("fec收到意料外的数据", slog.Any("channel id", sc.ChannelId))
			var frameBuf bytes.Buffer
			for i := 0; i < int(next.ShardCount); i++ {
				if len(next.Shards[i]) > 0 {
					frameBuf.Write(next.Shards[i])
				}
			}
			//slog.Debug("fec重组了一条数据", slog.Any("frame", frameBuf.String()))
			if sc.Channel != nil {
				sc.Channel <- StreamChannelData{
					ClientId:  sc.ClientId,
					ChannelId: sc.ChannelId,
					Offset:    0,
					Data:      frameBuf.Bytes(),
				}
			}
		}
	}
	return nil
}

func (sc *StreamChannel) GetFecEncoder(dataShards, parityShards uint8) (reedsolomon.Encoder, error) {
	if dataShards > 0 && parityShards > 0 {
		key := fmt.Sprintf("%d_%d", dataShards, parityShards)
		sc.lockEncoders.Lock()
		defer sc.lockEncoders.Unlock()
		if sc.FecEncoders[key] == nil {
			encoder, err := reedsolomon.New(int(dataShards), int(parityShards))
			if err != nil {
				return nil, err
			}
			sc.FecEncoders[key] = encoder
		}
		return sc.FecEncoders[key], nil
	}
	return nil, nil
}
