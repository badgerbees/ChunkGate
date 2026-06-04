//go:build windows

package limits

import (
	"syscall"
	"unsafe"
)

func AvailableBytes(path string) (int64, error) {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	proc := kernel32.NewProc("GetDiskFreeSpaceExW")
	path16, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var freeBytes uint64
	ret, _, callErr := proc.Call(
		uintptr(unsafe.Pointer(path16)),
		uintptr(unsafe.Pointer(&freeBytes)),
		0,
		0,
	)
	if ret == 0 {
		return 0, callErr
	}
	return int64(freeBytes), nil
}
