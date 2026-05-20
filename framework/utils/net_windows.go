package utils

import (
	"syscall"
)

func GetsockoptInt(fd uintptr, level, opt int) (int, error) {
	return syscall.GetsockoptInt(syscall.Handle(fd), level, opt)
}

func SetsockoptInt(fd uintptr, level, opt int, value int) (err error) {
	return syscall.SetsockoptInt(syscall.Handle(fd), level, opt, value)
}

func SetsockoptMin(fd uintptr, level, opt int, value int) (err error) {
	return syscall.SetsockoptInt(syscall.Handle(fd), level, opt, value)
}
