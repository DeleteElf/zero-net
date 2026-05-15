package main

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
)

func InitProcess() {
	if C.SetTimePeriod() != 0 {
		slog.Info("SetTimePeriod fail")
	} else {
		slog.Info("SetTimePeriod")
	}
}
