package utils

import (
	"io"
	"log/slog"
	"os"
)

func InitLog(level slog.Level, w io.Writer) {
	if w == nil {
		w = os.Stdout
	}

	addSource := false
	if level == slog.LevelDebug {
		addSource = true
	}
	handler := slog.NewTextHandler(w, &slog.HandlerOptions{
		Level:     level,     // 设置日志级别
		AddSource: addSource, // 包含源文件信息
	})
	slog.SetDefault(slog.New(handler))
	slog.Info("代理日志初始化完成！当前日志级别:" + level.String())
}
