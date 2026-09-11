package network

import (
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/klauspost/reedsolomon"
	"log/slog"
	"math"
	"time"
)

// FecGroup 用于收集和组装同一 GroupID 的分片
type FecGroup struct {
	HeaderSample    *FecPacketHeader
	HeaderTemplate  []byte
	Shards          [][]byte // 槽位数组，长度为 DataShards + ParityShards
	Packets         []*FecPacket
	Received        uint8     // 当前已收到的有效分片数
	HasParityShard  bool      //是否包含奇偶校验分片
	ExpiredAt       time.Time //预期销毁时间
	OosTime         time.Time //当前分组的首个数据包到达时间
	ShardCount      uint8     //当前分组的数据分片数量
	ShardDataLength uint16
}

// Depacketizer 解包器
type Depacketizer struct {
	Groups            map[uint8]*FecGroup
	CurrentFrameIndex uint32
	CurrentBlockIndex uint8
	CurrentGroupId    uint8
	//下一帧的序列
	NextSequenceNumber uint16
	// 是否正在等待下个关键帧
	WaitingForIdrFrame bool
	// 开始帧索引
	//StartFrameIndex uint32
	// 是否已经报告丢帧
	ReportedLostFrame bool
	//丢包数量
	MissingPackets uint16
	//是否收到Oos数据
	ReceivedOosData                   bool
	LastOosFramePresentationTimestamp uint64

	FecEncoderFactory
}

func NewDepacketizer() *Depacketizer {
	return &Depacketizer{
		Groups:         make(map[uint8]*FecGroup),
		CurrentGroupId: 1, //首次从1开始工作
		FecEncoderFactory: FecEncoderFactory{
			FecEncoders: make(map[string]reedsolomon.Encoder),
		},
	}
}

func (d *Depacketizer) JumpToNextIdrPacket(p *FecPacket) {
	if p.Header.BlockIdx == 0 && utils.IsBefore8(d.CurrentGroupId, p.Header.GroupIdx) { //如果是比当前更新的关键帧
		slog.Debug("帧接收新的关键帧，跳到！", slog.Any("channel", p.Header.ChannelId),
			slog.Any("groupId", p.Header.GroupIdx))
		target := int(p.Header.GroupIdx)
		if p.Header.GroupIdx < d.CurrentGroupId { //考虑溢出问题
			target = int(p.Header.GroupIdx) + math.MaxUint8
		}
		for i := int(d.CurrentGroupId); i < target; i++ {
			delete(d.Groups, uint8(i))
		}
		d.CurrentGroupId = p.Header.GroupIdx
		d.CurrentFrameIndex = p.Header.FrameIndex
		d.CurrentBlockIndex = 0
		d.MissingPackets = 0
		d.ReportedLostFrame = false
		d.ReceivedOosData = false
	}
}

func (d *Depacketizer) DoNextGroup(blockCount uint8) {
	delete(d.Groups, d.CurrentGroupId)
	d.CurrentGroupId++
	d.CurrentBlockIndex++
	d.MissingPackets = 0
	d.ReportedLostFrame = false
	d.ReceivedOosData = false
	if d.CurrentBlockIndex >= blockCount { //执行下一帧
		d.CurrentBlockIndex = 0
		d.CurrentFrameIndex++
	}
}

func (d *Depacketizer) RtpAddPacket(packet *FecPacket) *FecGroup {
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
		d.Groups[packet.Header.GroupIdx] = group
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
			slog.Debug("数据长度不一致，丢弃！", slog.Any("channel", packet.Header.ChannelId),
				slog.Any("ssrc", packet.Header.Ssrc),
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
			slog.Debug("进入无序状态", slog.Any("ssrc", packet.Header.Ssrc))
		}
	} else if d.ReceivedOosData && presentationTimeUs > d.LastOosFramePresentationTimestamp+SPECULATIVE_RFI_COOLDOWN_PERIOD_US { //从无序中恢复
		d.ReceivedOosData = false
		slog.Debug("退出无序状态", slog.Any("ssrc", packet.Header.Ssrc))
	}
	return group
}

//func (d *Depacketizer) Decode(sc *StreamChannel, p *FecPacket) error {
//	//slog.Debug("fec开始解包", slog.Any("channel id", sc.ChannelId), slog.Any("ssrc", p.Header.Ssrc), slog.Any("groupId", p.Header.GroupIdx))
//	totalShards := p.Header.DataShards + p.Header.ParityShards
//	if p.Header.ShardIdx >= totalShards {
//		return fmt.Errorf("无效的shard索引: %d", p.Header.ShardIdx)
//	}
//	//d, exists := sc.Depacketizers[p.Header.Ssrc]
//	//if !exists {
//	//	return fmt.Errorf("无效的通道数据: %d", p.Header.Ssrc)
//	//}
//	d.RtpAddPacket(p)
//	for {
//		next, exists := d.Groups[d.CurrentGroupId]
//		if !exists {
//			if p.Header.Header == 0x80 {
//				if utils.IsBefore8(d.CurrentGroupId, p.Header.GroupIdx) { //只需要处理数据包比当前待处理的还新，这一个问题
//					slog.Debug("收到了新的音频，但是不是期望的音频", slog.Any("target", d.CurrentGroupId),
//						slog.Any("received", p.Header.GroupIdx))
//					tempGroup := d.Groups[p.Header.GroupIdx]
//					if tempGroup.Received >= tempGroup.ShardCount { //新的已经接收满了
//						target := uint16(p.Header.GroupIdx)
//						if p.Header.GroupIdx < d.CurrentGroupId {
//							target += math.MaxUint8
//						}
//						for i := uint16(d.CurrentGroupId); i < target; i++ {
//							delete(d.Groups, uint8(i)) //清空缓存
//							//todo:这里可能需要补充空洞数据,如果不补，应该也可以，从时间维度来说，无形中，还能追帧，从效果来说，可能出现音爆
//							//group, exists := d.Groups[uint8(i)]
//							//if exists {
//							//	delete(d.Groups, uint8(i)) //清空缓存
//							//	for i := 0; i < int(group.ShardCount); i++ {
//							//		if group.Shards[i] == nil { //没有到的数据，补充一个空数据进去
//							//			group.Shards[i] = make([]byte, group.ShardDataLength) //音频数据不用重发，直接填满空洞即可
//							//		}
//							//		sc.handleDataToChannel(group.Shards[i])
//							//	}
//							//} else {
//							//	continue
//							//}
//						}
//						slog.Debug("新的音频数据已经满足解包，跳到最新音频数据", slog.Any("groupId", p.Header.GroupIdx))
//						d.CurrentGroupId = p.Header.GroupIdx //直接跳到当前，旧的全部舍弃
//						continue
//					}
//				}
//			}
//			break // 下一个组还没到来，退出循环
//		}
//		// 如果当前等待的组包数量还不足以解包，直接中断等待下一个网络包到达，切勿死循环！
//		header := next.HeaderSample //连续组装，不能使用packet，而应该从当前分组取样本
//		if next.Received < header.DataShards {
//			if p.Header.Header == 0x80 { //声音做简单的超时即可
//				if sc.CheckDataReceiveTimeout(next, d) {
//					continue
//				}
//			} else { //视频
//				if !d.ReportedLostFrame && !d.ReceivedOosData {
//					if d.MissingPackets > uint16(header.ParityShards) {
//						sc.notifyFrameLost(p.Header.Ssrc, d.CurrentFrameIndex, true)
//						d.ReportedLostFrame = true
//					}
//				}
//			}
//			break // 无论音频还是视频，数据未凑齐前退出循环，等待下一个 UDP 包
//		}
//		//事实证明数据凑齐了：检查是否打脸了之前的 Speculative RFI 误报
//		if d.ReportedLostFrame && !d.ReceivedOosData {
//			// 如果事实证明我们对主办方撒了谎，那就暂时停止进一步的推测性信息请求（RFI）。
//			d.ReceivedOosData = true
//			d.LastOosFramePresentationTimestamp = uint64(header.Timestamp) * 1000 / 90
//			slog.Debug("因误判丢帧退出 Speculative RFI 模式", slog.Any("frame", d.CurrentFrameIndex))
//		}
//
//		if next.HasParityShard { //包含奇偶校验分片，才执行
//			for i := 0; i < int(header.DataShards); i++ {
//				if next.Shards[i] == nil { // 丢失的分片：分配 maxLen 字节的零值切片供 RS 恢复
//					next.Shards[i] = make([]byte, next.ShardDataLength)
//				}
//				if len(next.Shards[i]) != int(next.ShardDataLength) {
//					slog.Debug("数据分片长度错误！", slog.Int("shardIndex", i),
//						slog.Any("targetLength", next.ShardDataLength),
//						slog.Int("shardDataLength", len(next.Shards[i])))
//				}
//			}
//			// 关键优化：使用 ReconstructData 仅恢复数据分片，比 Reconstruct 省时省 CPU
//			encoder, err := d.GetFecEncoder(header.DataShards, header.ParityShards)
//			if err != nil {
//				return fmt.Errorf("获取fec解包器出错【%d】: %w", header.GroupIdx, err)
//			}
//			err = encoder.ReconstructData(next.Shards)
//			if err != nil {
//				for i := 0; i < int(totalShards); i++ {
//					slog.Debug("解包报错，输出实际数据验证", slog.Int("shardIndex", i),
//						slog.Any("targetLength", next.ShardDataLength),
//						slog.Int("shardDataLength", len(next.Shards[i])))
//				}
//				d.DoNextGroup(next.HeaderSample.BlockCount)
//				return fmt.Errorf("fec解包出错【%d】:  %w", header.GroupIdx, err)
//			}
//			//slog.Debug("执行Fec解包逻辑", slog.Any("groupId", header.GroupIdx))
//		}
//		//slog.Debug("解包fec完成", slog.Int("groupId", int(next.GroupID)))
//		d.DoNextGroup(next.HeaderSample.BlockCount)
//
//		if next.HeaderTemplate[0] == RtpHeader || next.HeaderTemplate[0] == VideoHeader { //如果是rtp包
//			//这里可以根据特性进行拼接数据,如果考虑尽量零拷贝处理next.Shards
//			//现在这里有几个问题：
//			//1，我需要补充没有到的正规rtp 包的头信息 ，假设 一共6个数据包，数据分片是4个，当前到达的索引是 0,1,3,4，那么则需要补充序号是2的rtp头
//			//2，我需要告诉上层逻辑，fec已经处理完毕了
//			isVideo := next.HeaderTemplate[1] != 0x61 && next.HeaderTemplate[1] != 0x7f
//			//slog.Debug("fec开始重组", slog.Any("channel id", sc.ChannelId), slog.Any("groupId", header.GroupIdx))
//			for i := 0; i < int(header.DataShards); i++ {
//				if !isVideo { //先只修改音频支持
//					sc.handleDataToChannel(next.Shards[i])
//				} else {
//					var resultData []byte
//					if next.Packets[i] == nil { //Payload 是携带rtp包头信息的完整数据缓存
//						resultData = RebuildRtpPacket(next.HeaderTemplate, next.Shards[i], uint8(i), header.DataShards)
//					} else { //清除fecPercentage的数据
//						resultData = next.Packets[i].Payload //直接使用原始数据包，实现零拷贝
//					}
//					if isVideo { //因为我们已经处理过fec了，必须告诉上层没有fec分片数据了
//						oldFecInfo := binary.LittleEndian.Uint32(resultData[28:])
//						binary.LittleEndian.PutUint32(resultData[28:32], oldFecInfo&^(0x7F<<4))
//					}
//					//slog.Debug("fec重组了一条数据", slog.Any("channel id", sc.ChannelId), slog.Any("shard index", i))
//					sc.handleDataToChannel(resultData)
//				}
//			}
//			//slog.Debug("fec完成重组", slog.Any("channel id", sc.ChannelId), slog.Any("groupId", header.GroupIdx))
//		} else {
//			//slog.Debug("fec收到意料外的数据", slog.Any("channel id", sc.ChannelId))
//			var frameBuf bytes.Buffer
//			for i := 0; i < int(next.ShardCount); i++ {
//				if len(next.Shards[i]) > 0 {
//					frameBuf.Write(next.Shards[i])
//				}
//			}
//			//slog.Debug("fec重组了一条数据", slog.Any("frame", frameBuf.String()))
//			sc.handleDataToChannel(frameBuf.Bytes())
//		}
//	}
//	return nil
//}
