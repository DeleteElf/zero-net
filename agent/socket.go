package agent

import (
	"fmt"
	"net"
	"time"
)

const (
	PROXY_SIGN_VER  = "1"
	PROXY_SIGN_SALT = "2fbbdf99eae1675484a48e8310db1ee42d3bd6fdbc5e3f3755af848b23cc9817"

	AGENT_PKG_SIZE  = 4096
	AGENT_HEAD_SIZE = 4
	AGENT_PEER_SIZE = 16

	PROXY_MAGIC         = "CGTW"
	PROXY_FLAG_DEST_SVR = 0x01
	PROXY_FLAG_AUTH_REQ = 0x02
	PROXY_FLAG_AUTH_SYN = 0x04
)

type Socket struct {
	UdpConn   *net.UDPConn
	ReadData  []byte
	WriteData []byte
}

func (a *Socket) ReadFrom(p []byte) (int, net.Addr, error) {
	dataLen, addr, err := a.UdpConn.ReadFrom(a.ReadData)
	// slog.Info("ReadFrom", slog.Any("proxyAddr", addr), slog.Any("data", a.ReadData[:dataLen]))
	if dataLen <= AGENT_HEAD_SIZE {
		return 0, addr, err
	}

	readLen := dataLen - AGENT_HEAD_SIZE
	copy(p[0:readLen], a.ReadData[AGENT_HEAD_SIZE:dataLen])
	return readLen, addr, err
}

func (a *Socket) WriteTo(p []byte, addr net.Addr) (int, error) {
	// slog.Info("WriteTo", slog.Any("proxyAddr", addr), slog.Any("data", p))
	bufLen := AGENT_HEAD_SIZE + len(p)
	if bufLen > AGENT_PKG_SIZE {
		return -1, fmt.Errorf("too large")
	}
	copy(a.WriteData[AGENT_HEAD_SIZE:bufLen], p)

	writeLen, err := a.UdpConn.WriteTo(a.WriteData[0:bufLen], addr)
	if writeLen < AGENT_HEAD_SIZE {
		return writeLen, err
	}
	return writeLen - AGENT_HEAD_SIZE, err
}

func (a *Socket) Close() error {
	return a.UdpConn.Close()
}

func (a *Socket) LocalAddr() net.Addr {
	return a.UdpConn.LocalAddr()
}

func (a *Socket) SetDeadline(t time.Time) error {
	return a.UdpConn.SetDeadline(t)
}

func (a *Socket) SetReadDeadline(t time.Time) error {
	return a.UdpConn.SetReadDeadline(t)
}

func (a *Socket) SetWriteDeadline(t time.Time) error {
	return a.UdpConn.SetWriteDeadline(t)
}
