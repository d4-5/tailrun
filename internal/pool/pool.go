package pool

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

var (
	ErrWorkerNotFound = errors.New("worker not found")
)

type Broker interface {
	Publish(eventType string, data any)
}

type addWorkerReq struct {
	info  NewWorker
	reply chan int
}

type getWorkersReq struct {
	reply chan []WorkerInfo
}

type getWorkerResult struct {
	info WorkerInfo
	err  error
}

type getWorkerReq struct {
	id    int
	reply chan getWorkerResult
}

type getResourceUsageResult struct {
	usage []ResourceUsageInfo
	err   error
}

type getResourceUsageReq struct {
	id    int
	reply chan getResourceUsageResult
}

type getAvailableWorkerReq struct {
	reply chan Worker
}

type releaseWorkerReq struct {
	id int
}

type healthCheckResult struct {
	workerID int
	health   HealthResponse
	err      error
}

type HealthResponse struct {
	Status          string   `json:"status"`
	CPUUsagePercent *float64 `json:"cpuUsagePercent,omitempty"`
	MemoryUsed      *Bytes   `json:"memoryUsed,omitempty"`
	StorageUsed     *Bytes   `json:"storageUsed,omitempty"`
}

type Pool struct {
	workers              map[int]*worker
	nextID               int
	addWorkerCh          chan addWorkerReq
	getWorkersCh         chan getWorkersReq
	getWorkerCh          chan getWorkerReq
	getResourceUsageCh   chan getResourceUsageReq
	getAvailableWorkerCh chan getAvailableWorkerReq
	releaseWorkerCh      chan releaseWorkerReq
	healthCheckResultCh  chan healthCheckResult
	workersQueue         []int
	waitQueue            []chan Worker
	httpClient           *http.Client
	logger               *slog.Logger
	broker               Broker
}

func New(broker Broker, logger *slog.Logger) *Pool {
	p := &Pool{
		workers:              make(map[int]*worker),
		addWorkerCh:          make(chan addWorkerReq, 50),
		getWorkersCh:         make(chan getWorkersReq, 50),
		getWorkerCh:          make(chan getWorkerReq, 50),
		getResourceUsageCh:   make(chan getResourceUsageReq, 50),
		getAvailableWorkerCh: make(chan getAvailableWorkerReq, 50),
		releaseWorkerCh:      make(chan releaseWorkerReq, 50),
		healthCheckResultCh:  make(chan healthCheckResult, 50),
		httpClient:           &http.Client{Timeout: 10 * time.Second},
		logger:               logger,
		broker:               broker,
	}

	return p
}

func (p *Pool) Run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.startHealthChecks()
		case r := <-p.addWorkerCh:
			p.handleAddWorker(r)
		case r := <-p.getWorkersCh:
			p.handleGetWorkers(r)
		case r := <-p.getWorkerCh:
			p.handleGetWorker(r)
		case r := <-p.getResourceUsageCh:
			p.handleGetResourceUsage(r)
		case r := <-p.getAvailableWorkerCh:
			p.handleGetAvailableWorker(r)
		case r := <-p.releaseWorkerCh:
			p.handleReleaseWorker(r)
		case r := <-p.healthCheckResultCh:
			p.handleHealthCheckResult(r)
		case <-ctx.Done():
			return
		}
	}
}

func (p *Pool) startHealthChecks() {
	for _, w := range p.workers {
		if w.healthCheckInFlight {
			continue
		}
		w.healthCheckInFlight = true

		go p.checkWorkerHealth(w)
	}
}

func (p *Pool) checkWorkerHealth(w *worker) {
	health, err := w.checkHealth()
	p.healthCheckResultCh <- healthCheckResult{
		workerID: w.id,
		health:   health,
		err:      err,
	}
}

func (p *Pool) handleHealthCheckResult(r healthCheckResult) {
	w, ok := p.workers[r.workerID]
	if !ok {
		p.logger.Warn("received healthcheck result for unknown worker",
			"worker_id", r.workerID,
			"error", r.err,
		)
		return
	}

	w.healthCheckInFlight = false
	if r.err != nil {
		w.failedHealthChecks++
		p.logger.Warn("worker healthcheck failed",
			"worker_id", w.id,
			"failed_checks", w.failedHealthChecks,
			"error", r.err,
		)

		if w.failedHealthChecks >= 3 {
			p.setWorkerStatus(w, Dead)
			p.removeWorkerFromQueue(w.id)
		}
		return
	}

	w.failedHealthChecks = 0
	w.addResourceUsage(r.health)

	if w.status == Dead {
		p.setWorkerStatus(w, Available)
		p.workersQueue = append(p.workersQueue, w.id)
		p.trySendWorker()
	}
}

func (p *Pool) handleAddWorker(r addWorkerReq) {
	id := p.nextID
	p.nextID++
	w := &worker{
		id:         id,
		url:        r.info.URL,
		hostname:   r.info.Hostname,
		status:     Available,
		cpuCores:   r.info.CPUCores,
		os:         r.info.OS,
		memory:     r.info.Memory,
		storage:    r.info.Storage,
		httpClient: p.httpClient,
	}
	p.workers[id] = w
	p.workersQueue = append(p.workersQueue, id)

	p.broker.Publish(EventWorkerRegistered, WorkerRegisteredEvent{
		ID:       w.id,
		URL:      w.url,
		Hostname: w.hostname,
		Status:   w.status.String(),
		CPUCores: w.cpuCores,
		OS:       w.os,
		Memory:   w.memory,
		Storage:  w.storage,
	})

	p.trySendWorker()
	r.reply <- id
}

func (p *Pool) handleGetWorkers(r getWorkersReq) {
	infos := make([]WorkerInfo, 0, len(p.workers))
	for _, w := range p.workers {
		infos = append(infos, WorkerInfo{
			ID:       w.id,
			URL:      w.url,
			Hostname: w.hostname,
			Status:   w.status,
			CPUCores: w.cpuCores,
			OS:       w.os,
			Memory:   w.memory,
			Storage:  w.storage,
		})
	}
	r.reply <- infos
}

func (p *Pool) handleGetWorker(r getWorkerReq) {
	w, ok := p.workers[r.id]
	if !ok {
		r.reply <- getWorkerResult{err: ErrWorkerNotFound}
		return
	}
	r.reply <- getWorkerResult{
		info: WorkerInfo{
			ID:       w.id,
			URL:      w.url,
			Hostname: w.hostname,
			Status:   w.status,
			CPUCores: w.cpuCores,
			OS:       w.os,
			Memory:   w.memory,
			Storage:  w.storage,
		},
	}
}

func (p *Pool) handleGetResourceUsage(r getResourceUsageReq) {
	w, ok := p.workers[r.id]
	if !ok {
		r.reply <- getResourceUsageResult{err: ErrWorkerNotFound}
		return
	}

	usage := make([]ResourceUsageInfo, len(w.resourceUsage))
	for i, sample := range w.resourceUsage {
		usage[i] = ResourceUsageInfo{
			Timestamp:       sample.Timestamp,
			CPUUsagePercent: sample.CPUUsagePercent,
			MemoryUsed:      sample.MemoryUsed,
			StorageUsed:     sample.StorageUsed,
		}
	}
	r.reply <- getResourceUsageResult{usage: usage}
}

func (p *Pool) handleGetAvailableWorker(r getAvailableWorkerReq) {
	p.waitQueue = append(p.waitQueue, r.reply)
	p.trySendWorker()
}

func (p *Pool) handleReleaseWorker(r releaseWorkerReq) {
	w, ok := p.workers[r.id]
	if !ok {
		p.logger.Warn("failed to release worker", "worker_id", r.id, "error", ErrWorkerNotFound)
		return
	}

	if w.status != Busy {
		p.logger.Warn("worker already available", "worker_id", r.id)
		p.trySendWorker()
		return
	}

	p.setWorkerStatus(w, Available)
	p.workersQueue = append(p.workersQueue, w.id)
	p.trySendWorker()
}

func (p *Pool) trySendWorker() {
	for len(p.waitQueue) > 0 && len(p.workersQueue) > 0 {
		replyCh := p.waitQueue[0]
		p.waitQueue = p.waitQueue[1:]

		id := p.workersQueue[0]
		p.workersQueue = p.workersQueue[1:]

		w := p.workers[id]
		p.setWorkerStatus(w, Busy)
		replyCh <- w
	}
}

func (p *Pool) removeWorkerFromQueue(workerID int) {
	filtered := make([]int, 0, len(p.workersQueue))

	for _, id := range p.workersQueue {
		if id != workerID {
			filtered = append(filtered, id)
		}
	}

	p.workersQueue = filtered
}

func (p *Pool) setWorkerStatus(w *worker, status WorkerStatus) {
	if w.status == status {
		return
	}

	w.status = status
	p.broker.Publish(EventWorkerUpdated, WorkerUpdatedEvent{
		ID:     w.id,
		Status: w.status.String(),
	})
}

func (p *Pool) AddWorker(info NewWorker) int {
	reply := make(chan int, 1)
	p.addWorkerCh <- addWorkerReq{info: info, reply: reply}
	return <-reply
}

func (p *Pool) GetWorkers() []WorkerInfo {
	reply := make(chan []WorkerInfo, 1)
	p.getWorkersCh <- getWorkersReq{reply: reply}
	return <-reply
}

func (p *Pool) GetWorker(id int) (WorkerInfo, error) {
	reply := make(chan getWorkerResult, 1)
	p.getWorkerCh <- getWorkerReq{id: id, reply: reply}
	res := <-reply
	return res.info, res.err
}

func (p *Pool) GetResourceUsage(id int) ([]ResourceUsageInfo, error) {
	reply := make(chan getResourceUsageResult, 1)
	p.getResourceUsageCh <- getResourceUsageReq{id: id, reply: reply}
	res := <-reply
	return res.usage, res.err
}

func (p *Pool) AvailableWorker() <-chan Worker {
	reply := make(chan Worker, 1)
	p.getAvailableWorkerCh <- getAvailableWorkerReq{reply: reply}
	return reply
}

func (p *Pool) ReleaseWorker(id int) {
	p.releaseWorkerCh <- releaseWorkerReq{id: id}
}
