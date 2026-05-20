package utils

import (
	"syscall"
)

func GetsockoptInt(fd uintptr, level, opt int) (int, error) {
	return syscall.GetsockoptInt(int(fd), level, opt)
}

func SetsockoptInt(fd uintptr, level, opt int, value int) (err error) {
	return syscall.SetsockoptInt(int(fd), level, opt, value)
}

func SetsockoptMin(fd uintptr, level, opt int, value int) (err error) {
	newValue := 0
	curSize, _ := syscall.GetsockoptInt(int(fd), level, opt)
	if curSize > 0 {
		if curSize < value {
			newValue = value
		}
	} else {
		newValue = value
	}
	if newValue > 0 {
		return syscall.SetsockoptInt(int(fd), level, opt, value)
	}
	return nil
}
