package pool

import "errors"

const maxConsecutiveHealthCheckErrors = 3

var ErrWorkerDied = errors.New("worker died")

type WorkerError struct {
	WorkerID int
	Err      error
}

type getResourceUsageResult struct {
	usage []ResourceUsageInfo
	err   error
}

type getResourceUsageReq struct {
	id    int
	reply chan getResourceUsageResult
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

		if w.failedHealthChecks >= maxConsecutiveHealthCheckErrors && w.status != Dead {
			p.setWorkerStatus(w, Dead)
			p.removeWorkerFromQueue(w.id)
			p.workerErrorCh <- WorkerError{
				WorkerID: w.id,
				Err:      ErrWorkerDied,
			}
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

func (p *Pool) WorkerError() <-chan WorkerError {
	return p.workerErrorCh
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

func (p *Pool) GetResourceUsage(id int) ([]ResourceUsageInfo, error) {
	reply := make(chan getResourceUsageResult, 1)
	p.getResourceUsageCh <- getResourceUsageReq{id: id, reply: reply}
	res := <-reply
	return res.usage, res.err
}
