package pool

import "uuid"

type getAvailableWorkerReq struct {
	reply chan Worker
}

type releaseWorkerReq struct {
	id uuid.UUID
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
		p.logger.Warn("worker is not busy",
			"worker_id", r.id,
			"status", w.status,
		)
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

func (p *Pool) removeWorkerFromQueue(workerID uuid.UUID) {
	filtered := make([]uuid.UUID, 0, len(p.workersQueue))

	for _, id := range p.workersQueue {
		if id != workerID {
			filtered = append(filtered, id)
		}
	}

	p.workersQueue = filtered
}

func (p *Pool) setWorkerStatus(w *worker, status WorkerStatus) {
	w.status = status
	p.broker.Publish(EventWorkerUpdated, WorkerUpdatedEvent{
		ID:     w.id,
		Status: w.status.String(),
	})
}

func (p *Pool) AvailableWorker() <-chan Worker {
	reply := make(chan Worker, 1)
	p.getAvailableWorkerCh <- getAvailableWorkerReq{reply: reply}
	return reply
}

func (p *Pool) ReleaseWorker(id uuid.UUID) {
	p.releaseWorkerCh <- releaseWorkerReq{id: id}
}
