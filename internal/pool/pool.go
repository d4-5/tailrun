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
