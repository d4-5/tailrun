package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
)

type SysInfo struct {
	Hostname string
	OS       string
	CPUCores int
	Memory   Bytes
	Storage  Bytes
}

type SysLoad struct {
	CPUUsagePercent float64
	MemoryUsed      Bytes
	StorageUsed     Bytes
}

func getSysInfo() (SysInfo, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return SysInfo{}, err
	}

	memory, err := getTotalMemory()
	if err != nil {
		return SysInfo{}, fmt.Errorf("failed to get memory: %w", err)
	}

	storage, err := getTotalStorage()
	if err != nil {
		return SysInfo{}, fmt.Errorf("failed to get storage: %w", err)
	}

	return SysInfo{
		Hostname: hostname,
		OS:       runtime.GOOS,
		CPUCores: runtime.NumCPU(),
		Memory:   memory,
		Storage:  storage,
	}, nil
}

func getSysLoad() (SysLoad, error) {
	cpuUsage, err := getCPUUsage()
	if err != nil {
		return SysLoad{}, fmt.Errorf("failed to get CPU usage: %w", err)
	}

	memoryUsed, err := getUsedMemory()
	if err != nil {
		return SysLoad{}, fmt.Errorf("failed to get memory used: %w", err)
	}

	storageUsed, err := getUsedStorage()
	if err != nil {
		return SysLoad{}, fmt.Errorf("failed to get storage used: %w", err)
	}

	return SysLoad{
		CPUUsagePercent: cpuUsage,
		MemoryUsed:      memoryUsed,
		StorageUsed:     storageUsed,
	}, nil
}

func getTotalMemory() (Bytes, error) {
	switch runtime.GOOS {
	case "linux":
		return getLinuxTotalMemory()
	default:
		return 0, fmt.Errorf("memory detection not implemented for %s", runtime.GOOS)
	}
}

func getLinuxTotalMemory() (Bytes, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}

	var memTotal Bytes
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			var kb uint64
			if _, err := fmt.Sscanf(line, "MemTotal: %d kB", &kb); err == nil {
				memTotal = Bytes(kb * 1024)
				break
			}
		}
	}

	if memTotal == 0 {
		return 0, fmt.Errorf("failed to parse MemTotal from /proc/meminfo")
	}
	return memTotal, nil
}

func getTotalStorage() (Bytes, error) {
	switch runtime.GOOS {
	case "linux":
		return getLinuxTotalStorage()
	default:
		return 0, fmt.Errorf("storage detection not implemented for %s", runtime.GOOS)
	}
}

func getLinuxTotalStorage() (Bytes, error) {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return 0, err
	}

	var total Bytes
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "/dev/") || strings.Contains(line, " / ") {
			parts := strings.Fields(line)
			if len(parts) >= 2 && parts[1] == "/" {
				total, err = getBlockDeviceSize()
				if err == nil {
					return total, nil
				}
			}
		}
	}

	var statfs syscall.Statfs_t
	if err := syscall.Statfs("/", &statfs); err == nil {
		return Bytes(statfs.Blocks) * Bytes(statfs.Bsize), nil
	}

	return 0, fmt.Errorf("failed to get total storage")
}

func getBlockDeviceSize() (Bytes, error) {
	var statfs syscall.Statfs_t
	if err := syscall.Statfs("/", &statfs); err == nil {
		return Bytes(statfs.Blocks) * Bytes(statfs.Bsize), nil
	}
	return 0, fmt.Errorf("failed to get block device size")
}

func getCPUUsage() (float64, error) {
	switch runtime.GOOS {
	case "linux":
		return getLinuxCPUUsage()
	default:
		return 0, fmt.Errorf("CPU usage detection not implemented for %s", runtime.GOOS)
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
		return 0, fmt.Errorf("memory used detection not implemented for %s", runtime.GOOS)
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
		return 0, fmt.Errorf("storage used detection not implemented for %s", runtime.GOOS)
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
