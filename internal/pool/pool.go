package pool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

var (
	ErrWorkerNotFound = errors.New("worker not found")
)

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

func (s WorkerStatus) String() (string, error) {
	switch s {
	case Available:
		return "available", nil
	case Busy:
		return "busy", nil
	default:
		return "", errors.New("unknown worker status")
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
	memory     Bytes
	storage    Bytes
	httpClient *http.Client
}

type NewWorker struct {
	URL      string
	Hostname string
	CPUCores int
	OS       string
	Memory   Bytes
	Storage  Bytes
}

type WorkerInfo struct {
	ID       int
	URL      string
	Hostname string
	Status   WorkerStatus
	CPU      int
	OS       string
	Memory   Bytes
	Storage  Bytes
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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("worker returned status %d", resp.StatusCode)
	}

	return nil
}

func (w *worker) ID() int {
	return w.id
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

type getAvailableWorkerReq struct {
	reply chan Worker
}

type releaseWorkerReq struct {
	id int
}

type Pool struct {
	workers              map[int]*worker
	nextID               int
	addWorkerCh          chan addWorkerReq
	getWorkersCh         chan getWorkersReq
	getWorkerCh          chan getWorkerReq
	getAvailableWorkerCh chan getAvailableWorkerReq
	releaseWorkerCh      chan releaseWorkerReq
	workersQueue         []int
	waitQueue            []chan Worker
	httpClient           *http.Client
	logger               *slog.Logger
}

func New(logger *slog.Logger) *Pool {
	p := &Pool{
		workers:              make(map[int]*worker),
		addWorkerCh:          make(chan addWorkerReq, 50),
		getWorkersCh:         make(chan getWorkersReq, 50),
		getWorkerCh:          make(chan getWorkerReq, 50),
		getAvailableWorkerCh: make(chan getAvailableWorkerReq, 50),
		releaseWorkerCh:      make(chan releaseWorkerReq, 50),
		httpClient:           &http.Client{Timeout: 10 * time.Second},
		logger:               logger,
	}

	return p
}

func (p *Pool) Run(ctx context.Context) {
	for {
		select {
		case r := <-p.addWorkerCh:
			p.handleAddWorker(r)
		case r := <-p.getWorkersCh:
			p.handleGetWorkers(r)
		case r := <-p.getWorkerCh:
			p.handleGetWorker(r)
		case r := <-p.getAvailableWorkerCh:
			p.handleGetAvailableWorker(r)
		case r := <-p.releaseWorkerCh:
			p.handleReleaseWorker(r)
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
			CPU:      w.cpuCores,
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
			CPU:      w.cpuCores,
			OS:       w.os,
			Memory:   w.memory,
			Storage:  w.storage,
		},
	}
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

	w.status = Available
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
		w.status = Busy
		replyCh <- w
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

func (p *Pool) AvailableWorker() <-chan Worker {
	reply := make(chan Worker, 1)
	p.getAvailableWorkerCh <- getAvailableWorkerReq{reply: reply}
	return reply
}

func (p *Pool) ReleaseWorker(id int) {
	p.releaseWorkerCh <- releaseWorkerReq{id: id}
}
