package pool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
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
)

func (s WorkerStatus) String() string {
	switch s {
	case Available:
		return "available"
	case Busy:
		return "busy"
	default:
		panic(fmt.Sprintf("pool: unknown WorkerStatus value: %d", s))
	}
}

type Bytes uint64

type worker struct {
	id         int
	url        string
	hostname   string
	status     WorkerStatus
	cpuCores   int
	os         string
	memory     *Bytes
	storage    *Bytes
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
