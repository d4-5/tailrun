package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

var (
	ErrUnsupported = errors.New("system metric unsupported")
)

type SysInfo struct {
	Hostname string
	OS       string
	CPUCores int
	Memory   *Bytes
	Storage  *Bytes
}

type SysLoad struct {
	CPUUsagePercent float64
	MemoryUsed      Bytes
	StorageUsed     Bytes
}

func getSysInfo() (SysInfo, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return SysInfo{}, fmt.Errorf("get hostname: %w", err)
	}

	info := SysInfo{
		Hostname: hostname,
		OS:       runtime.GOOS,
		CPUCores: runtime.NumCPU(),
	}

	memory, err := getMemory()
	if err != nil {
		if !errors.Is(err, ErrUnsupported) {
			return SysInfo{}, fmt.Errorf("get memory: %w", err)
		}
	} else {
		info.Memory = &memory
	}

	storage, err := getStorage()
	if err != nil {
		if !errors.Is(err, ErrUnsupported) {
			return SysInfo{}, fmt.Errorf("get storage: %w", err)
		}
	} else {
		info.Storage = &storage
	}

	return info, nil
}

func getMemory() (Bytes, error) {
	switch runtime.GOOS {
	case "linux":
		return getLinuxMemory()
	default:
		return 0, ErrUnsupported
	}
}

func getLinuxMemory() (Bytes, error) {
	var info unix.Sysinfo_t

	if err := unix.Sysinfo(&info); err != nil {
		return 0, fmt.Errorf("get system memory: %w", err)
	}

	return Bytes(info.Totalram) * Bytes(info.Unit), nil
}

func getStorage() (Bytes, error) {
	switch runtime.GOOS {
	case "linux":
		return getLinuxStorage()
	default:
		return 0, ErrUnsupported
	}
}

func getLinuxStorage() (Bytes, error) {
	var stat unix.Statfs_t

	if err := unix.Statfs("/", &stat); err != nil {
		return 0, fmt.Errorf("get filesystem stats: %w", err)
	}

	return Bytes(stat.Blocks) * Bytes(stat.Bsize), nil
}

func getSysLoad() (SysLoad, error) {
	cpuUsage, err := getCPUUsage()
	if err != nil {
		return SysLoad{}, fmt.Errorf("get cpu usage: %w", err)
	}

	memoryUsed, err := getUsedMemory()
	if err != nil {
		return SysLoad{}, fmt.Errorf("get memory used: %w", err)
	}

	storageUsed, err := getUsedStorage()
	if err != nil {
		return SysLoad{}, fmt.Errorf("get storage used: %w", err)
	}

	return SysLoad{
		CPUUsagePercent: cpuUsage,
		MemoryUsed:      memoryUsed,
		StorageUsed:     storageUsed,
	}, nil
}

func getCPUUsage() (float64, error) {
	switch runtime.GOOS {
	case "linux":
		return getLinuxCPUUsage()
	default:
		return 0, ErrUnsupported
	}
}

func getLinuxCPUUsage() (float64, error) {
	return 0, fmt.Errorf("CPU usage calculation requires state tracking")
}

func getUsedMemory() (Bytes, error) {
	switch runtime.GOOS {
	case "linux":
		return getLinuxUsedMemory()
	default:
		return 0, ErrUnsupported
	}
}

func getLinuxUsedMemory() (Bytes, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}

	var memTotal, memAvailable uint64
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			_, _ = fmt.Sscanf(line, "MemTotal: %d kB", &memTotal)
		} else if strings.HasPrefix(line, "MemAvailable:") {
			_, _ = fmt.Sscanf(line, "MemAvailable: %d kB", &memAvailable)
		} else if strings.HasPrefix(line, "MemFree:") && memAvailable == 0 {
			_, _ = fmt.Sscanf(line, "MemFree: %d kB", &memAvailable)
		}
	}

	if memTotal == 0 {
		return 0, fmt.Errorf("failed to parse memory info")
	}

	usedKB := memTotal - memAvailable
	return Bytes(usedKB * 1024), nil
}

func getUsedStorage() (Bytes, error) {
	switch runtime.GOOS {
	case "linux":
		return getLinuxUsedStorage()
	default:
		return 0, ErrUnsupported
	}
}

func getLinuxUsedStorage() (Bytes, error) {
	var statfs syscall.Statfs_t
	if err := syscall.Statfs("/", &statfs); err != nil {
		return 0, err
	}

	total := Bytes(statfs.Blocks) * Bytes(statfs.Bsize)
	free := Bytes(statfs.Bfree) * Bytes(statfs.Bsize)
	used := total - free
	return used, nil
}
