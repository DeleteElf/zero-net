package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/quic-go/quic-go"
	"io"
	"log/slog"
	"math"
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
	Depacketizers map[uint8]*Depacketizer
	Packetizers   map[uint8]*Packetizer
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
		CloseableObject: framework.CloseableObject{
			IsClosed: false,
		},
	}
	sc.Depacketizers[0] = NewDepacketizer() //初始化一个解包器
	sc.Packetizers[0] = NewPacketizer()     //初始化一个打包器
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
		sc.handleDataToChannel(buf)
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

func (sc *StreamChannel) CheckDataReceiveTimeout(group *FecGroup, depacketizer *Depacketizer) bool {
	if group.ExpiredAt.Before(time.Now()) { //如果已经过期，则不再等待，直接接收下一个
		if group.HeaderTemplate[0] == RtpHeader && //音频数据是陆续发送的，我们允许按时间递增等待
			(group.HeaderTemplate[1] == AudioHeader || group.HeaderTemplate[1] == AudioDynamicHeader) {
			//expireTime := group.OosTime.Add(AudioOosExpireTime + time.Duration(group.ShardCount-1)*AudioDataTime)
			//if expireTime.Before(time.Now()) {
			//	slog.Debug("音频帧，oos检测，数据接收超时丢弃！", slog.Int("channel", sc.ChannelId),
			//		slog.Any("groupId", depacketizer.CurrentGroupId), slog.Any("已接收", group.Received),
			//		slog.Any("合计", len(group.Shards)))
			//todo:这里需要补充一下数据并告诉上层逻辑
			for i := 0; i < int(group.ShardCount); i++ {
				if group.Shards[i] == nil { //没有到的数据，补充一个空数据进去
					group.Shards[i] = make([]byte, group.ShardDataLength) //音频数据不用重发，直接填满空洞即可
				}
				sc.handleDataToChannel(group.Shards[i])
			}
			//	depacketizer.DoNextGroup(group.HeaderSample.BlockCount)
			//	return true
			//}
			//return false
		} else if group.HeaderTemplate[0] == VideoHeader { //如果是我们的视频包
			//expireTime := group.OosTime.Add(VideoOosExpireTime)
			//if expireTime.Before(time.Now()) {
			//	slog.Debug("视频帧，oos检测，数据接收超时丢弃！", slog.Int("channel", sc.ChannelId),
			//		slog.Any("groupId", depacketizer.CurrentGroupId), slog.Any("已接收", group.Received),
			//		slog.Any("合计", len(group.Shards)))
			//	depacketizer.DoNextGroup(group.HeaderSample.BlockCount)
			//	return true
			//}
			return false //如果没有超过
		}
		slog.Debug("帧接收超时丢弃！", slog.Int("channel", sc.ChannelId),
			slog.Any("groupId", depacketizer.CurrentGroupId), slog.Any("已接收", group.Received),
			slog.Any("合计", len(group.Shards)))
		depacketizer.DoNextGroup(group.HeaderSample.BlockCount)
		return true
	}
	return false
}

func (sc *StreamChannel) FecDecode(packet *FecPacket) error {
	//slog.Debug("fec开始解包", slog.Any("channel id", sc.ChannelId), slog.Any("ssrc", packet.Header.Ssrc), slog.Any("groupId", packet.Header.GroupIdx))
	totalShards := packet.Header.DataShards + packet.Header.ParityShards
	if packet.Header.ShardIdx >= totalShards {
		return fmt.Errorf("无效的shard索引: %d", packet.Header.ShardIdx)
	}
	depacketizer, exists := sc.Depacketizers[packet.Header.Ssrc]
	if !exists {
		return fmt.Errorf("无效的通道数据: %d", packet.Header.Ssrc)
	}
	depacketizer.RtpAddPacket(packet)
	for {
		nextGroup, exists := depacketizer.Groups[depacketizer.CurrentGroupId]
		if !exists {
			if packet.Header.Header == 0x80 { //音频数据包
				if utils.IsBefore8(depacketizer.CurrentGroupId, packet.Header.GroupIdx) { //只需要处理数据包比当前待处理的还新，这一个问题
					slog.Debug("收到了新的音频，但是不是期望的音频", slog.Any("target", depacketizer.CurrentGroupId),
						slog.Any("received", packet.Header.GroupIdx))
					tempGroup := depacketizer.Groups[packet.Header.GroupIdx]
					if tempGroup.Received >= tempGroup.ShardCount { //新的已经接收满了
						target := uint16(packet.Header.GroupIdx)
						if packet.Header.GroupIdx < depacketizer.CurrentGroupId {
							target += math.MaxUint8
						}
						for i := uint16(depacketizer.CurrentGroupId); i < target; i++ {
							delete(depacketizer.Groups, uint8(i)) //清空缓存
							//todo:这里可能需要补充空洞数据,如果不补，应该也可以，从时间维度来说，无形中，还能追帧，从效果来说，可能出现音爆
						}
						slog.Debug("新的音频数据已经满足解包，跳到最新音频数据", slog.Any("groupId", packet.Header.GroupIdx))
						depacketizer.CurrentGroupId = packet.Header.GroupIdx //直接跳到当前，旧的全部舍弃
						continue
					}
				}
			}
			break // 下一个组还没到来，退出循环
		}
		// 如果当前等待的组包数量还不足以解包，直接中断等待下一个网络包到达，切勿死循环！
		header := nextGroup.HeaderSample //连续组装，不能使用packet，而应该从当前分组取样本
		if nextGroup.Received < header.DataShards {
			if packet.Header.Header == 0x80 { //声音做简单的超时即可
				if sc.CheckDataReceiveTimeout(nextGroup, depacketizer) {
					continue
				}
			} else {                                                                                          //视频数据包
				if depacketizer.StartFrameIndex != depacketizer.CurrentFrameIndex && nextGroup.Received > 0 { //重新计算当前分组的丢包情况
					outOfSequence := false
					seqNumber := (nextGroup.MaxSequenceNumber - nextGroup.StartSequenceNumber + 1) & 0xFFFF
					if seqNumber != uint16(nextGroup.Received) { //检查当前待处理的数据包 是否是当前分组的最大序列
						outOfSequence = true
						depacketizer.MissingPackets += seqNumber - uint16(nextGroup.Received) //计算丢帧数量
					}
					depacketizer.NextSequenceNumber = (nextGroup.MaxSequenceNumber + 1) & 0xFFFF //ps：这里直接跳到了最终
					maxIdx := nextGroup.MaxSequenceNumber - nextGroup.StartSequenceNumber
					p := nextGroup.Packets[uint8(maxIdx)]
					if p == nil {
						slog.Debug("出意外了！！！！")
						for i := int(maxIdx) - 1; i >= 0; i-- {
							if p == nil {
								p = nextGroup.Packets[uint8(i)]
								continue
							}
							break
						}
					}
					if p != nil { //如果p一直为空，则等待下一次继续处理。
						presentationTimeUs := uint64(p.Header.Timestamp) * 1000 / 90
						if outOfSequence { //无序状态的数据，我们记录一下
							depacketizer.LastOosFramePresentationTimestamp = presentationTimeUs
							if !depacketizer.ReceivedOosData { //接收到无序的数据了
								depacketizer.ReceivedOosData = true
							}
						}
						depacketizer.StartFrameIndex = depacketizer.CurrentFrameIndex //更新到当前帧
					}
				}
			}
			break // 无论音频还是视频，数据未凑齐前退出循环，等待下一个 UDP 包
		}
		//事实证明数据凑齐了：检查是否打脸了之前的 推测的 RFI 误报
		if depacketizer.ReportedLostFrame && !depacketizer.ReceivedOosData { //如果报告了丢帧，但是又恢复了有序，则表示误报
			// 如果事实证明我们对主办方撒了谎，那就暂时停止进一步的推测性信息请求（RFI）。
			depacketizer.ReceivedOosData = true
			depacketizer.LastOosFramePresentationTimestamp = uint64(header.Timestamp) * 1000 / 90
			slog.Debug("因误判丢帧退出 推测的 RFI 模式，进入无序状态", slog.Any("frame", depacketizer.CurrentFrameIndex))
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
			encoder, err := depacketizer.GetFecEncoder(header.DataShards, header.ParityShards)
			if err != nil {
				return fmt.Errorf("获取fec解包器出错【%d】: %w", header.GroupIdx, err)
			}
			err = encoder.ReconstructData(nextGroup.Shards)
			if err != nil {
				for i := 0; i < int(totalShards); i++ {
					slog.Debug("解包报错，输出实际数据验证", slog.Int("shardIndex", i),
						slog.Any("targetLength", nextGroup.ShardDataLength),
						slog.Int("shardDataLength", len(nextGroup.Shards[i])))
				}
				depacketizer.DoNextGroup(nextGroup.HeaderSample.BlockCount)
				return fmt.Errorf("fec解包出错【%d】:  %w", header.GroupIdx, err)
			}
			//slog.Debug("执行Fec解包逻辑", slog.Any("groupId", header.GroupIdx))
		}
		//slog.Debug("解包fec完成", slog.Int("groupId", int(nextGroup.GroupID)))
		depacketizer.DoNextGroup(nextGroup.HeaderSample.BlockCount)

		if nextGroup.HeaderTemplate[0] == RtpHeader || nextGroup.HeaderTemplate[0] == VideoHeader { //如果是rtp包
			//这里可以根据特性进行拼接数据,如果考虑尽量零拷贝处理next.Shards
			//现在这里有几个问题：
			//1，我需要补充没有到的正规rtp 包的头信息 ，假设 一共6个数据包，数据分片是4个，当前到达的索引是 0,1,3,4，那么则需要补充序号是2的rtp头
			//2，我需要告诉上层逻辑，fec已经处理完毕了
			isVideo := nextGroup.HeaderTemplate[1] != 0x61 && nextGroup.HeaderTemplate[1] != 0x7f
			//slog.Debug("fec开始重组", slog.Any("channel id", sc.ChannelId), slog.Any("groupId", header.GroupIdx))
			for i := 0; i < int(header.DataShards); i++ {
				if !isVideo { //先只修改音频支持
					//slog.Debug("执行音频直接接收并解包", slog.Int("size", len(nextGroup.Shards[i])))
					sc.handleDataToChannel(nextGroup.Shards[i]) //直接使用原始数据包，实现零拷贝
				} else {
					var resultData []byte
					if nextGroup.Packets[i] == nil { //Payload 是携带rtp包头信息的完整数据缓存
						resultData = RebuildRtpPacket(nextGroup.HeaderTemplate, nextGroup.Shards[i], uint8(i), header.DataShards)
					} else { //清除fecPercentage的数据
						resultData = nextGroup.Packets[i].Payload //直接使用原始数据包，实现零拷贝
					}
					if isVideo { //因为我们已经处理过fec了，必须告诉上层没有fec分片数据了
						oldFecInfo := binary.LittleEndian.Uint32(resultData[28:])
						binary.LittleEndian.PutUint32(resultData[28:32], oldFecInfo&^(0x7F<<4))
					}
					//slog.Debug("fec重组了一条数据", slog.Any("channel id", sc.ChannelId), slog.Any("shard index", i))
					sc.handleDataToChannel(resultData)
				}
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
			sc.handleDataToChannel(frameBuf.Bytes())
		}
	}
	return nil
}

func (sc *StreamChannel) handleDataToChannel(data []byte) {
	if sc.Channel != nil {
		sc.Channel <- StreamChannelData{
			ClientId:  sc.ClientId,
			ChannelId: sc.ChannelId,
			Offset:    0,
			Data:      data,
		}
	}
}
