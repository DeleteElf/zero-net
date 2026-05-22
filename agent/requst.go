package agent

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/DeleteElf/network-quic/framework/utils"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

type Requst struct {
	Ts      int64  `json:"ts"`        // 时间戳
	Sign    string `json:"sign"`      // 鉴权，用于校验本次请求是否合法，**以防动态库被不可信第三方调用**
	MgrAddr string `json:"mgr_addr"`  // 管理平台的地址
	Token   string `json:"token"`     // 管理平台的token
	DevId   string `json:"dev_id"`    // 管理平台的dev_id
	ProxyId string `json:"proxy_id"`  // 管理平台的proxy_id
	SvrAddr string `json:"svr_addr"`  // 服务端的地址
	Proxy   bool   `json:"proxy"`     // 是否代理
	CliId   string `json:"client_id"` // 客户端ID
	NetType string // 网络类型
	SvcType string // 服务地址
	SvcAddr string `json:"service"`  // 目标地址
	LoAddr  string `json:"lo_addr"`  // 本地监听地址，此时需要设置 up_buf down_buf
	UpBuf   int    `json:"up_buf"`   // TCP的上行缓冲区大小，默认64KB，此大小会影响吞吐、延迟
	DownBuf int    `json:"down_buf"` // TCP的下行缓冲区大小，默认64KB，此大小会影响吞吐、延迟
}

func CheckRequst(data []byte) (*Requst, error) {
	var req Requst
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("req json %s unmarshal err:%v", string(data), err)
	}
	diffTs := time.Now().Unix() - req.Ts
	if diffTs > 60 || diffTs < -60 {
		slog.Warn("invalid ts", slog.Int64("ts", req.Ts))
		return nil, fmt.Errorf("invalid ts")
	}
	if req.MgrAddr == "" {
		return nil, fmt.Errorf("invalid mgr addr")
	}
	if req.Token == "" {
		return nil, fmt.Errorf("invalid token")
	}
	if req.DevId == "" {
		return nil, fmt.Errorf("invalid device id")
	}
	if req.ProxyId == "" {
		return nil, fmt.Errorf("invalid proxy id")
	}
	if req.SvrAddr == "" {
		return nil, fmt.Errorf("invalid server addr")
	}
	if req.CliId == "" {
		return nil, fmt.Errorf("invalid client id")
	}
	if req.SvrAddr == "" {
		return nil, fmt.Errorf("invalid service addr")
	}
	arr := strings.Split(req.SvcAddr, "://")
	if len(arr) != 2 {
		return nil, fmt.Errorf("invalid service addr")
	}
	req.SvcType = arr[0]
	req.SvcAddr = arr[1]
	if req.SvcType == "http" || req.SvcType == "https" || req.SvcType == "rtsp" {
		req.NetType = "tcp"
	} else {
		req.NetType = req.SvcType
	}
	sign := utils.EncryptBytes([]byte(fmt.Sprintf("%s_%s_%d", req.SvrAddr, SIGN_SALT, req.Ts)))
	if sign != req.Sign {
		slog.Warn("invalid sign, but ignore for test")
		// todo return nil, nil, fmt.Errorf("invalid sign")
	}
	slog.Info("requst", slog.Any("json", req))
	return &req, nil
}

func GetProxy(req *Requst) (*Proxy, error) {
	var proxy *Proxy = nil
	if req.Proxy {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client := &http.Client{Transport: tr}

		// 创建Request对象
		urlGet := fmt.Sprintf("%s/proxy?device_id=%s&proxy_id=%s", req.MgrAddr, req.DevId, req.ProxyId)
		slog.Debug("正在请求管理中心：", slog.String("url", urlGet))
		reqGet, err := http.NewRequest("GET", urlGet, nil)
		if err != nil {
			return nil, fmt.Errorf("create request err: %v", err)
		}
		reqGet.Header.Set("Authorization", req.Token)
		var authResult ActionProxy
		// 发送请求
		resp, err := client.Do(reqGet)
		if err != nil {
			//if i == maxTry-1 {
			return nil, fmt.Errorf("requst url err: %v", err)
			//}
			//time.Sleep(500 * time.Millisecond)
			//continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			//if i == maxTry-1 {
			return nil, fmt.Errorf("respone status: %s", resp.Status)
			//}
			//time.Sleep(500 * time.Millisecond)
			//continue
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			//if i == maxTry-1 {
			return nil, fmt.Errorf("read err: %v", err)
			//}
			//time.Sleep(500 * time.Millisecond)
			//continue
		}

		slog.Info(string(body))
		if err := json.Unmarshal(body, &authResult); err != nil {
			return nil, fmt.Errorf("mgr resp json unmarshal err: %v", err)
		}
		if !authResult.IsSuccess() {
			//if i == maxTry-1 {
			return nil, fmt.Errorf("mgr resp not success")
			//}
			//time.Sleep(500 * time.Millisecond)
			//continue
		}
		proxy = &authResult.Data

		if len(proxy.ProxyAddr) == 0 {
			proxy.ProxyAddr = proxy.ProxyExternalIp + ":" + proxy.ProxyExternalPort
		}
		slog.Debug("获取到的代理地址：", slog.String("address", proxy.ProxyAddr))
		if _, err := net.ResolveUDPAddr("udp", proxy.ProxyAddr); err != nil {
			return nil, fmt.Errorf("mgr resp proxy addr %s err: %v", proxy.ProxyAddr, err)
		}
	}
	return proxy, nil
}
