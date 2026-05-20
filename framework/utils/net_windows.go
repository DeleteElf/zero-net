package utils

/*
#cgo windows LDFLAGS: -lWinmm

#include <windows.h>

int SetTimePeriod() {
    if (timeBeginPeriod(1) == TIMERR_NOERROR) {
        return 0;
    }
    return -1;
}
*/
import "C"

import (
	"log/slog"
	"syscall"
)

func InitProcess() {
	if C.SetTimePeriod() != 0 {
		slog.Info("SetTimePeriod fail")
	} else {
		slog.Info("SetTimePeriod")
	}
}

func GetsockoptInt(fd uintptr, level, opt int) (int, error) {
	return syscall.GetsockoptInt(syscall.Handle(fd), level, opt)
}

func SetsockoptInt(fd uintptr, level, opt int, value int) (err error) {
	return syscall.SetsockoptInt(syscall.Handle(fd), level, opt, value)
}

func SetsockoptMin(fd uintptr, level, opt int, value int) (err error) {
	return syscall.SetsockoptInt(syscall.Handle(fd), level, opt, value)
}
