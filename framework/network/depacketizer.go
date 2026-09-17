package network

import (
	"bytes"
	"encoding/binary"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/klauspost/reedsolomon"
	"io"
	"log/slog"
	"math"
	"sync"
	"time"
)

// FecGroup 用于收集和组装同一 GroupID 的分片
type FecGroup struct {
	HeaderSample   *FecPacketHeader
	HeaderTemplate []byte
	Shards         [][]byte // 槽位数组，长度为 DataShards + ParityShards
	Packets        []*FecPacket
	Received       uint8 // 当前已收到的有效分片数
	HasParityShard bool  //是否包含奇偶校验分片
	//ExpiredAt        time.Time //预期销毁时间
	OosTimeExpiredAt time.Time //当前分组的首个数据包到达时间
	ShardCount       uint8     //当前分组的数据分片数量
	ShardDataLength  uint16
	//计算起始的第一个
	StartSequenceNumber uint16
	//最大的序列号
	MaxSequenceNumber uint16
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
	// 开始帧索引，用于丢包统计
	StartFrameIndex uint32
	//当前收到的最大帧
	MaxFrameIndex uint32
	//丢包数量
	MissingPackets uint16
	//重新构建的序列
	RebuildSequenceNumber uint16
	// 是否已经报告丢帧
	ReportedLostFrame bool
	//是否收到Oos数据
	ReceivedOosData bool
	//上个Oos数据的报告时间
	LastOosFramePresentationTimestamp uint64

	//关键帧是否已经处理
	IdrFrameProcessed bool
	FecEncoderFactory
	framework.CloseableObject

	groupLock     sync.Mutex
	StreamChannel *StreamChannel
}

func NewDepacketizer(sc *StreamChannel) *Depacketizer {
	d := &Depacketizer{
		Groups:         make(map[uint8]*FecGroup),
		CurrentGroupId: 1, //首次从1开始工作
		StreamChannel:  sc,
		FecEncoderFactory: FecEncoderFactory{
			FecEncoders: make(map[string]reedsolomon.Encoder),
		},
		CloseableObject: framework.CloseableObject{
			Closed: false,
		},
	}
	go d.Decode()
	return d
}

func (d *Depacketizer) OnClosing() bool {
	return true
}

func (d *Depacketizer) OnClosed() error {
	d.groupLock.Lock()
	defer d.groupLock.Unlock()
	clear(d.Groups)
	clear(d.FecEncoders)
	return nil
}

func (d *Depacketizer) JumpToNextPacket(p *FecPacket) {
	if p.Header.BlockIdx == 0 && utils.IsBefore8(d.CurrentGroupId, p.Header.GroupIdx) { //如果是比当前更新的关键帧
		if p.Header.Idr == 1 {
			slog.Debug("接收新的关键帧，跳到目标帧！", slog.Any("channel", p.Header.ChannelId), slog.Any("groupId", p.Header.GroupIdx))
		} else {
			slog.Debug("跳到目标帧！", slog.Any("channel", p.Header.ChannelId), slog.Any("groupId", p.Header.GroupIdx))
		}
		d.jumpToNextGroupInternal(p.Header.GroupIdx, p.Header.FrameIndex)
	}
}

func (d *Depacketizer) JumpToNextGroup(groupId uint8, frameIndex uint32) {
	if utils.IsBefore8(d.CurrentGroupId, groupId) { //只能往后跳，还最多只能跳127
		slog.Debug("跳帧到下个分组", slog.Any("groupId", groupId))
		d.jumpToNextGroupInternal(groupId, frameIndex)
	}
}

func (d *Depacketizer) jumpToNextGroupInternal(groupId uint8, frameIndex uint32) {
	d.groupLock.Lock()
	defer d.groupLock.Unlock()
	start := d.CurrentGroupId - d.CurrentBlockIndex
	target := int(groupId)
	if groupId < start { //考虑溢出问题
		target = int(groupId) + math.MaxUint8
	}
	for i := int(start); i < target; i++ {
		delete(d.Groups, uint8(i))
	}
	d.CurrentGroupId = groupId
	d.CurrentFrameIndex = frameIndex //如果有用，需要后续自己修复
	d.CurrentBlockIndex = 0          //如果有用，需要后续自己修复
	d.MissingPackets = 0
}

func (d *Depacketizer) DoNextGroup(blockCount uint8) {
	d.groupLock.Lock()
	defer d.groupLock.Unlock()
	d.CurrentGroupId++
	d.CurrentBlockIndex++
	d.MissingPackets = 0
	if d.CurrentBlockIndex >= blockCount { //执行下一帧
		for i := 0; i < int(d.CurrentBlockIndex); i++ { //删除当前帧的缓存
			delete(d.Groups, d.CurrentGroupId-d.CurrentBlockIndex+uint8(i))
		}
		d.CurrentBlockIndex = 0
		d.CurrentFrameIndex++
	}
}

func (d *Depacketizer) RtpAddPacket(packet *FecPacket) bool {
	d.groupLock.Lock()
	group, exists := d.Groups[packet.Header.GroupIdx]
	if !exists { //第2中情况是循环了一圈回来，仍没有被处置？ || group.HeaderSample.FrameIndex != packet.Header.FrameIndex
		totalShards := packet.Header.DataShards + packet.Header.ParityShards
		group = &FecGroup{
			HeaderSample: &packet.Header, OosTimeExpiredAt: time.Now().Add(AudioOosExpireTime), // ExpiredAt: time.Now().Add(expireTime),
			Shards: make([][]byte, totalShards), Packets: make([]*FecPacket, totalShards),
			ShardCount: packet.Header.DataShards, ShardDataLength: packet.Header.Length,
			HeaderTemplate: packet.HeaderRaw,
			//GroupID: packet.Header.GroupIdx, DataShards: packet.Header.DataShards, ParityShards: packet.Header.ParityShards,
			//Total: packet.Header.Total, Received: 0, CreatedAt: time.Now(),
		}
		d.Groups[packet.Header.GroupIdx] = group
	}
	d.groupLock.Unlock()
	if packet.Header.ShardIdx >= group.HeaderSample.DataShards+group.HeaderSample.ParityShards { //校验数据要使用分组的数据进行校验
		slog.Debug("无效的shard索引", slog.Any("channel", packet.Header.ChannelId), slog.Any("ssrc", packet.Header.Ssrc),
			slog.Any("groupId", packet.Header.GroupIdx), slog.Any("ShardIdx", packet.Header.ShardIdx),
			slog.Any("DataShards", packet.Header.DataShards), slog.Any("ParityShards", packet.Header.ParityShards),
			slog.Any("SequenceNumber", packet.Header.SequenceNumber), slog.Any("frameIndex", packet.Header.FrameIndex),

			slog.Any("CurrentGroupId", d.CurrentGroupId), slog.Any("NextSequenceNumber", d.NextSequenceNumber),

			slog.Any("Received", group.Received), slog.Any("groupFrameIndex", group.HeaderSample.FrameIndex),
			slog.Any("groupDataShards", group.HeaderSample.DataShards), slog.Any("groupParityShards", group.HeaderSample.ParityShards))
		return false
	}
	if group.Packets[packet.Header.ShardIdx] == nil { //不接收一样的数据包,通过Packet来判断，shards因为需要用于恢复，这里不进行判断
		group.Shards[packet.Header.ShardIdx] = packet.Payload[packet.Header.HeaderSize:] //只加入验证过的数据
		group.Packets[packet.Header.ShardIdx] = packet                                   // 记录原始包指针
		if !group.HasParityShard && packet.Header.ShardIdx >= packet.Header.DataShards {
			group.HasParityShard = true
		}
		if group.Received == 0 { //接收第一个计算一次就好了
			group.StartSequenceNumber = packet.Header.SequenceNumber - uint16(packet.Header.ShardIdx)
			group.MaxSequenceNumber = packet.Header.SequenceNumber //首个直接赋值，主要是当序列执行到  65535的一半之后，如果没有正确赋值首个，则会产生逻辑错误
		} else if utils.IsBefore16(group.MaxSequenceNumber, packet.Header.SequenceNumber) { //非首个数据才执行判定逻辑
			group.MaxSequenceNumber = packet.Header.SequenceNumber //更新最大序列
		}
		group.Received++
		if packet.Header.GroupIdx == d.CurrentGroupId { //如果等于当前分组我们需要计算丢包率
			outOfSequence := false
			if packet.Header.SequenceNumber != d.NextSequenceNumber {
				outOfSequence = true
				//slog.Debug("收到无序帧", slog.Int("channel", sc.ChannelId), slog.Any("group", packet.Header.GroupIdx),
				//	slog.Any("SequenceNumber", packet.Header.SequenceNumber))
				if utils.IsBefore16(packet.Header.SequenceNumber, d.NextSequenceNumber) { //比下个小，就补充一个包，减少一个丢包
					if d.MissingPackets > 0 { //若这是补入的乱序包，修正 MissingPacket
						d.MissingPackets--
					}
				} else {
					// 收到比预期大的 Seq，说明中间发生了缺包，累加 MissingPackets
					d.MissingPackets += packet.Header.SequenceNumber - d.NextSequenceNumber
					d.NextSequenceNumber = packet.Header.SequenceNumber + 1 //ps：这里直接跳到了最终
				}
			} else { //正常递增，不减少丢包
				d.NextSequenceNumber++
			}
			presentationTimeUs := uint64(packet.Header.Timestamp) * 1000 / 90
			if outOfSequence { //无序状态的数据，我们记录一下
				d.LastOosFramePresentationTimestamp = presentationTimeUs
				if !d.ReceivedOosData { //接收到无序的数据了
					d.ReceivedOosData = true
					slog.Debug("出现乱序，退出推测性RFI状态", slog.Any("ssrc", packet.Header.Ssrc), slog.Any("groupId", d.CurrentGroupId))
				}
			} else if d.ReceivedOosData && presentationTimeUs > d.LastOosFramePresentationTimestamp+SPECULATIVE_RFI_COOLDOWN_PERIOD_US { //从无序中恢复
				d.ReceivedOosData = false
				slog.Debug("恢复顺序，进入推测性RFI状态", slog.Any("ssrc", packet.Header.Ssrc), slog.Any("groupId", d.CurrentGroupId))
			}
		}
		return true
	}
	return false
}

func (d *Depacketizer) processAudioPacket(group *FecGroup) {
	if d.StreamChannel == nil {
		slog.Debug("解包器未设置流通道，请先设置！")
		return
	}
	switch d.StreamChannel.Level {
	case FecDepacketizeKeepRtpData:
		for i := 0; i < int(group.ShardCount); i++ {
			if group.Shards[i] == nil { //没有到的数据，补充一个空数据进去
				group.Shards[i] = make([]byte, group.ShardDataLength) //音频数据不用重发，直接填满空洞即可
			}
			d.StreamChannel.handleReaderToChannel(group.HeaderSample.Ssrc, bytes.NewReader(group.Shards[i]), int(group.ShardDataLength))
		}
	case FecDepacketizeKeepRtpPacket, FecDepacketizeKeepRtpPacketAndSize: //音频的处置方式
		//case FecDepacketizeKeepRtpPacketAndSize:
		for i := 0; i < int(group.ShardCount); i++ {
			var resultData []byte
			if group.Packets[i] == nil {
				if d.StreamChannel.Level == FecDepacketizeKeepRtpPacketAndSize {
					if group.Shards[i] == nil { //没有到的数据，补充一个空数据进去
						group.Shards[i] = make([]byte, group.ShardDataLength) //音频数据不用重发，直接填满空洞即可
					}
					resultData = RebuildRtpPacket(group.HeaderTemplate, group.Shards[i], uint8(i), group.HeaderSample.DataShards)
				} else { //处理超时，直接丢入一个rtp空包
					resultData = RebuildRtpPacket(group.HeaderTemplate, []byte{}, uint8(i), group.HeaderSample.DataShards)
				}
			} else { //清除fecPercentage的数据
				resultData = group.Packets[i].Payload //直接使用原始数据包，实现零拷贝
			}
			d.StreamChannel.handleReaderToChannel(group.HeaderSample.Ssrc, bytes.NewReader(resultData), len(resultData))
		}
	default:
	}
}

func (d *Depacketizer) CheckDataReceiveTimeout(group *FecGroup) bool {
	if group.OosTimeExpiredAt.Before(time.Now()) { //如果已经过期，则不再等待，直接接收下一个
		d.processAudioPacket(group)
		slog.Debug("音频帧接收超时丢弃！", slog.Any("channel", group.HeaderSample.ChannelId),
			slog.Any("groupId", d.CurrentGroupId), slog.Any("已接收", group.Received),
			slog.Any("合计", len(group.Shards)))
		d.DoNextGroup(group.HeaderSample.BlockCount)
		return true
	}
	return false
}

func (d *Depacketizer) Decode() {
	for {
		if d.Closed { //关闭了退出
			return
		}
		if d.StreamChannel == nil || d.StreamChannel.Closed { //通道关闭也退出
			return
		}

		for {
			d.groupLock.Lock()
			for key, group := range d.Groups { //执行循环删除，防止早期数据污染引发问题
				if utils.IsBefore8(group.HeaderSample.GroupIdx, d.CurrentGroupId) {
					delete(d.Groups, key)
				}
			}
			nextGroup, exists := d.Groups[d.CurrentGroupId]
			d.groupLock.Unlock()
			if !exists {
				time.Sleep(time.Millisecond * 1) //数据没来就睡眠1毫秒
				break                            // 下一个组还没到来，退出循环
			}
			// 如果当前等待的组包数量还不足以解包，直接中断等待下一个网络包到达，切勿死循环！
			header := nextGroup.HeaderSample //连续组装，不能使用packet，而应该从当前分组取样本
			totalShards := header.DataShards + header.ParityShards
			if nextGroup.Received < header.DataShards {
				if header.Header == 0x80 { //声音做简单的超时即可
					if d.CheckDataReceiveTimeout(nextGroup) {
						continue
					}
				} else {
					if d.StartFrameIndex != d.CurrentFrameIndex && nextGroup.Received > 0 { //重新计算当前分组的丢包情况
						outOfSequence := false
						count := (nextGroup.MaxSequenceNumber - nextGroup.StartSequenceNumber + 1) & 0xFFFF
						if count != uint16(nextGroup.Received) { //检查当前待处理的数据包 是否是当前分组的最大序列
							outOfSequence = true
							d.MissingPackets += count - uint16(nextGroup.Received) //计算丢帧数量
						}
						d.NextSequenceNumber = (nextGroup.MaxSequenceNumber + 1) & 0xFFFF //ps：这里直接跳到了最终
						maxIdx := nextGroup.MaxSequenceNumber - nextGroup.StartSequenceNumber
						if int(maxIdx) > len(nextGroup.Packets) {
							slog.Debug("数据判定逻辑产生了越界，会导致崩溃，这里捕获辅助调试！", slog.Any("start", nextGroup.StartSequenceNumber),
								slog.Any("end", nextGroup.MaxSequenceNumber))
						} else {
							p := nextGroup.Packets[maxIdx]
							if p != nil { //如果p一直为空，则等待下一次继续处理。
								presentationTimeUs := uint64(p.Header.Timestamp) * 1000 / 90
								if outOfSequence { //无序状态的数据，我们记录一下
									d.LastOosFramePresentationTimestamp = presentationTimeUs //更新乱序时间
									if !d.ReceivedOosData {                                  //接收到无序的数据了
										d.ReceivedOosData = true
									}
								}
								d.StartFrameIndex = d.CurrentFrameIndex //更新到当前帧
							} else {
								slog.Debug("数据包逻辑错误，意料外的空值！")
							}
						}
					}
					if !d.ReportedLostFrame && !d.ReceivedOosData {
						if d.MissingPackets > uint16(header.ParityShards) {
							d.StreamChannel.notifyFrameLost(header.Ssrc, d.CurrentFrameIndex, true)
							d.ReportedLostFrame = true
						}
					}
				}
				time.Sleep(time.Millisecond * 1) //数据量不足也睡眠等待1毫秒
				break                            // 无论音频还是视频，数据未凑齐前退出循环，等待下一个 UDP 包
			}

			//事实证明数据凑齐了：检查是否打脸了之前的 推测的 RFI 误报
			if d.ReportedLostFrame && !d.ReceivedOosData { //如果报告了丢帧，但是又恢复了有序，则表示误报
				// 如果事实证明我们对主办方撒了谎，那就暂时停止进一步的推测性信息请求（RFI）。
				d.ReceivedOosData = true
				d.LastOosFramePresentationTimestamp = uint64(header.Timestamp) * 1000 / 90
				slog.Debug("因误判丢帧退出 推测的 RFI 模式，进入无序状态", slog.Any("frame", d.CurrentFrameIndex))
			}

			if nextGroup.HasParityShard { //包含奇偶校验分片，才执行
				for i := 0; i < int(header.DataShards); i++ {
					if nextGroup.Shards[i] == nil { // 丢失的分片：分配 maxLen 字节的零值切片供 RS 恢复
						nextGroup.Shards[i] = make([]byte, nextGroup.ShardDataLength)
					}
					if len(nextGroup.Shards[i]) != int(nextGroup.ShardDataLength) {
						slog.Debug("数据分片长度错误！", slog.Int("shardIndex", i),
							slog.Any("targetLength", nextGroup.ShardDataLength),
							slog.Int("shardDataLength", len(nextGroup.Shards[i])))
					}
				}
				// 关键优化：使用 ReconstructData 仅恢复数据分片，比 Reconstruct 省时省 CPU
				encoder, err := d.GetFecEncoder(header.DataShards, header.ParityShards)
				if err != nil {
					slog.Error("获取fec解包器出错", slog.Any("GroupIdx", header.GroupIdx), slog.Any("err", err))
					time.Sleep(time.Millisecond * 1) //等待1毫秒，再继续
					break
				}
				err = encoder.ReconstructData(nextGroup.Shards)
				if err != nil {
					for i := 0; i < int(totalShards); i++ {
						slog.Debug("解包报错，输出实际数据验证", slog.Int("shardIndex", i),
							slog.Any("targetLength", nextGroup.ShardDataLength),
							slog.Int("shardDataLength", len(nextGroup.Shards[i])))
					}
					d.DoNextGroup(nextGroup.HeaderSample.BlockCount)
					slog.Error("fec解包出错", slog.Any("GroupIdx", header.GroupIdx), slog.Any("err", err))
					time.Sleep(time.Millisecond * 1) //等待1毫秒，再继续
					break
				}
				//slog.Debug("执行Fec解包逻辑", slog.Any("groupId", header.GroupIdx))
			}
			d.DoNextGroup(nextGroup.HeaderSample.BlockCount) //解包成功就直接进入下一帧，免得再接收多余的帧
			//暂时只支持97和127的音频
			if nextGroup.HeaderTemplate[0] == RtpHeader && (nextGroup.HeaderTemplate[1] == AudioHeader || nextGroup.HeaderTemplate[1] == AudioDynamicHeader) {
				d.processAudioPacket(nextGroup)
			} else if nextGroup.HeaderTemplate[0] == VideoHeader { //如果是rtp包 //暂时只支持 0x90的格式
				//这里可以根据特性进行拼接数据,如果考虑尽量零拷贝处理next.Shards
				//现在这里有几个问题：
				//1，我需要补充没有到的正规rtp 包的头信息 ，假设 一共6个数据包，数据分片是4个，当前到达的索引是 0,1,3,4，那么则需要补充序号是2的rtp头
				//2，我需要告诉上层逻辑，fec已经处理完毕了
				switch d.StreamChannel.Level {
				case FecDepacketizeKeepRtpData: //全部合成成一帧数据
					//slog.Debug("执行Fec解包成功，正在执行FecDepacketizeKeepRtpData", slog.Any("groupId", header.GroupIdx))
					if nextGroup.HeaderSample.BlockIdx == nextGroup.HeaderSample.BlockCount-1 { //确保是最后一个分块
						totalShardCount := uint16(0)
						totalSize := 0
						startGroupId := nextGroup.HeaderSample.GroupIdx - nextGroup.HeaderSample.BlockCount + 1
						d.groupLock.Lock()
						for i := uint8(0); i < nextGroup.HeaderSample.BlockCount; i++ {
							totalShardCount += uint16(d.Groups[startGroupId+i].ShardCount)
						}
						index := 0
						readers := make([]io.Reader, int(totalShardCount))
						for i := uint8(0); i < nextGroup.HeaderSample.BlockCount; i++ {
							group := d.Groups[startGroupId+i]
							for j := uint8(0); j < group.ShardCount; j++ {
								totalSize += int(group.ShardDataLength)
								readers[index] = bytes.NewReader(group.Shards[j])
								index++
							}
						}
						d.groupLock.Unlock()
						streamReader := io.MultiReader(readers...)
						d.StreamChannel.handleReaderToChannel(nextGroup.HeaderSample.Ssrc, streamReader, totalSize)
					}
				case FecDepacketizeKeepRtpPacket: //保留block数据，我们只处理group内的数据
					//同帧的数据，header、frameIndex、ssrc、multiFecFlags、multiFecBlocks、timestamp一样，
					//sequenceNumber需要重建，sequenceNumber从0开始
					totalSize := VideoHeaderLength + int(nextGroup.ShardDataLength)*int(nextGroup.ShardCount)
					readers := make([]io.Reader, int(nextGroup.ShardCount)+1)
					var resultData []byte
					if nextGroup.Packets[0] == nil {
						resultData = RebuildRtpPacket(nextGroup.HeaderTemplate, []byte{}, uint8(0), header.DataShards)[:VideoHeaderLength]
					} else { //清除fecPercentage的数据
						resultData = nextGroup.Packets[0].Payload[:VideoHeaderLength] //直接使用原始数据包，实现零拷贝
					}
					binary.BigEndian.PutUint16(resultData[2:], d.RebuildSequenceNumber) //写入新的SequenceNumber
					resultData[24] = 0x7
					binary.LittleEndian.PutUint32(resultData[28:], uint32(1)<<22)
					//slog.Debug("执行Fec解包成功，正在执行FecDepacketizeKeepRtpPacket", slog.Any("ssrc", header.Ssrc),
					//	slog.Any("groupId", header.GroupIdx), slog.Any("SequenceNumber", d.RebuildSequenceNumber))
					d.RebuildSequenceNumber++
					readers[0] = bytes.NewReader(resultData)
					for j := uint8(0); j < nextGroup.ShardCount; j++ {
						readers[j+1] = bytes.NewReader(nextGroup.Shards[j])
					}
					streamReader := io.MultiReader(readers...)
					d.StreamChannel.handleReaderToChannel(nextGroup.HeaderSample.Ssrc, streamReader, totalSize)
				case FecDepacketizeKeepRtpPacketAndSize:
					//slog.Debug("执行Fec解包成功，正在执行FecDepacketizeKeepRtpPacketAndSize", slog.Any("groupId", header.GroupIdx))
					for i := 0; i < int(header.DataShards); i++ {
						//方案1：中间版本的逻辑，需要重建rtp，其实，我们连rtp都不需要重建
						var resultData []byte
						if nextGroup.Packets[i] == nil {
							resultData = RebuildRtpPacket(nextGroup.HeaderTemplate, nextGroup.Shards[i], uint8(i), header.DataShards)
						} else { //清除fecPercentage的数据
							resultData = nextGroup.Packets[i].Payload //直接使用原始数据包，实现零拷贝
						}
						if len(resultData) >= 32 {
							oldFecInfo := binary.LittleEndian.Uint32(resultData[28:])
							binary.LittleEndian.PutUint32(resultData[28:32], oldFecInfo&^(0x7F<<4))
						} else {
							slog.Warn("数据包长度不足，跳过位标志清除", slog.Int("len", len(resultData)))
						}
						//slog.Debug("正在处理视频数据包", slog.Any("ssrc", nextGroup.HeaderSample.Ssrc), slog.Int("数据长度", len(resultData)),
						//	slog.Any("内容", resultData))
						d.StreamChannel.handleReaderToChannel(nextGroup.HeaderSample.Ssrc, bytes.NewReader(resultData), len(resultData))
					}
				default:
				}
				if header.Idr == 1 {
					slog.Debug("关键帧解码完成，向服务器发送ltr_ack！", slog.Any("ssrc", header.Ssrc), slog.Any("目标帧", header.FrameIndex))
					d.StreamChannel.connectionReceivedCompleteFrame(header.Ssrc, header.FrameIndex, true)
				}
				//slog.Debug("fec完成重组", slog.Any("channel id", sc.ChannelId), slog.Any("groupId", header.GroupIdx))
			} else {
				//slog.Debug("fec收到意料外的数据", slog.Any("channel id", sc.ChannelId))
				var frameBuf bytes.Buffer
				for i := 0; i < int(nextGroup.ShardCount); i++ {
					if len(nextGroup.Shards[i]) > 0 {
						frameBuf.Write(nextGroup.Shards[i])
					}
				}
				//slog.Debug("fec重组了一条数据", slog.Any("frame", frameBuf.String()))
				d.StreamChannel.handleReaderToChannel(nextGroup.HeaderSample.Ssrc, bytes.NewReader(frameBuf.Bytes()), frameBuf.Len())
			}
		}
	}
}

//func (d *Depacketizer) Decode(sc *DataStreamChannel, p *FecPacket) error {
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
