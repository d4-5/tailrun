package pool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	EventWorkerRegistered = "worker_registered"
	EventWorkerUpdated    = "worker_updated"
)

type WorkerUpdatedEvent struct {
	ID     int    `json:"id"`
	Status string `json:"status"`
}

type WorkerRegisteredEvent struct {
	ID       int    `json:"id"`
	URL      string `json:"url"`
	Hostname string `json:"hostname"`
	Status   string `json:"status"`
	CPUCores int    `json:"cpuCores"`
	OS       string `json:"os"`
	Memory   *Bytes `json:"memory,omitempty"`
	Storage  *Bytes `json:"storage,omitempty"`
}

type Worker interface {
	Execute(task Task) error
	ID() int
}

type Task struct {
	ID      int               `json:"id"`
	Command string            `json:"command"`
	EnvVars map[string]string `json:"envVars"`
}

type WorkerStatus int

const (
	Available WorkerStatus = iota
	Busy
	Dead
)

func (s WorkerStatus) String() string {
	switch s {
	case Available:
		return "available"
	case Busy:
		return "busy"
	case Dead:
		return "dead"
	default:
		panic(fmt.Sprintf("pool: unknown WorkerStatus value: %d", s))
	}
}

type Bytes uint64

const maxResourceUsageSamples = 50

type resourceUsage struct {
	Timestamp       time.Time
	CPUUsagePercent *float64
	MemoryUsed      *Bytes
	StorageUsed     *Bytes
}

type ResourceUsageInfo struct {
	Timestamp       time.Time
	CPUUsagePercent *float64
	MemoryUsed      *Bytes
	StorageUsed     *Bytes
}

type worker struct {
	id       int
	url      string
	hostname string
	status   WorkerStatus
	cpuCores int
	os       string
	memory   *Bytes
	storage  *Bytes

	healthCheckInFlight bool
	failedHealthChecks  int

	resourceUsage []resourceUsage

	httpClient *http.Client
}

type NewWorker struct {
	URL      string
	Hostname string
	CPUCores int
	OS       string
	Memory   *Bytes
	Storage  *Bytes
}

type WorkerInfo struct {
	ID       int
	URL      string
	Hostname string
	Status   WorkerStatus
	CPUCores int
	OS       string
	Memory   *Bytes
	Storage  *Bytes
}

func (w *worker) Execute(task Task) error {
	body, err := json.Marshal(task)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		w.url+"/tasks",
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("worker returned status %d", resp.StatusCode)
	}

	return nil
}

func (w *worker) ID() int {
	return w.id
}

func (w *worker) addResourceUsage(health HealthResponse) {
	w.resourceUsage = append(w.resourceUsage, resourceUsage{
		Timestamp:       time.Now(),
		CPUUsagePercent: health.CPUUsagePercent,
		MemoryUsed:      health.MemoryUsed,
		StorageUsed:     health.StorageUsed,
	})
	if len(w.resourceUsage) <= maxResourceUsageSamples {
		return
	}

	firstRecent := len(w.resourceUsage) - maxResourceUsageSamples
	oldLength := len(w.resourceUsage)
	copy(w.resourceUsage, w.resourceUsage[firstRecent:])
	clear(w.resourceUsage[maxResourceUsageSamples:oldLength])
	w.resourceUsage = w.resourceUsage[:maxResourceUsageSamples]
}

func (w *worker) checkHealth() (HealthResponse, error) {
	var health HealthResponse

	req, err := http.NewRequest(
		http.MethodGet,
		w.url+"/health",
		nil,
	)
	if err != nil {
		return health, err
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return health, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return health, fmt.Errorf("worker returned health status %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return health, fmt.Errorf("decode health response: %w", err)
	}

	return health, nil
}
