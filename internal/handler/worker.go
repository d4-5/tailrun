package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/n9cw/tailrun/internal/pool"
)

type Workers interface {
	AddWorker(info pool.NewWorker) int
	GetWorkers() []pool.WorkerInfo
	GetWorker(id int) (pool.WorkerInfo, error)
}

type RegisterWorkerRequest struct {
	URL      string     `json:"url"`
	Hostname string     `json:"hostname"`
	CPUCores int        `json:"cpuCores"`
	OS       string     `json:"os"`
	Memory   pool.Bytes `json:"memory"`
	Storage  pool.Bytes `json:"storage"`
}

type RegisterWorkerResponse struct {
	ID int `json:"id"`
}

type WorkerResponse struct {
	ID       int        `json:"id"`
	URL      string     `json:"url"`
	Hostname string     `json:"hostname"`
	Status   string     `json:"status"`
	CPUCores int        `json:"cpuCores"`
	OS       string     `json:"os"`
	Memory   pool.Bytes `json:"memory"`
	Storage  pool.Bytes `json:"storage"`
}

type WorkerHandler struct {
	workers Workers
}

func NewWorkerHandler(workers Workers) *WorkerHandler {
	return &WorkerHandler{workers: workers}
}

func (h *WorkerHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/workers", h.HandleRegisterWorker)
	mux.HandleFunc("GET /api/workers", h.HandleGetWorkers)
	mux.HandleFunc("GET /api/workers/{id}", h.HandleGetWorker)
}

func (h *WorkerHandler) HandleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req RegisterWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.URL == "" {
		writeError(w, http.StatusBadRequest, "url is required")
		return
	}
	if req.Hostname == "" {
		writeError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	if req.OS == "" {
		writeError(w, http.StatusBadRequest, "os is required")
		return
	}
	if req.CPUCores <= 0 {
		writeError(w, http.StatusBadRequest, "number of cpu cores must be greater than 0")
		return
	}
	if req.Memory == 0 {
		writeError(w, http.StatusBadRequest, "memory must be greater than 0 bytes")
		return
	}
	if req.Storage == 0 {
		writeError(w, http.StatusBadRequest, "storage must be greater than 0 bytes")
		return
	}

	id := h.workers.AddWorker(pool.NewWorker{
		URL:      req.URL,
		Hostname: req.Hostname,
		CPUCores: req.CPUCores,
		OS:       req.OS,
		Memory:   req.Memory,
		Storage:  req.Storage,
	})

	writeJSON(w, http.StatusCreated, RegisterWorkerResponse{ID: id})
}

func (h *WorkerHandler) HandleGetWorkers(w http.ResponseWriter, r *http.Request) {
	workers := h.workers.GetWorkers()

	resp := make([]WorkerResponse, 0, len(workers))
	for _, worker := range workers {
		wr := WorkerResponse{
			ID:       worker.ID,
			URL:      worker.URL,
			Hostname: worker.Hostname,
			Status:   worker.Status.String(),
			CPUCores: worker.CPUCores,
			OS:       worker.OS,
			Memory:   worker.Memory,
			Storage:  worker.Storage,
		}
		resp = append(resp, wr)
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *WorkerHandler) HandleGetWorker(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid worker id")
		return
	}

	worker, err := h.workers.GetWorker(id)
	if err != nil {
		if errors.Is(err, pool.ErrWorkerNotFound) {
			writeError(w, http.StatusNotFound, "worker not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	wr := WorkerResponse{
		ID:       worker.ID,
		URL:      worker.URL,
		Hostname: worker.Hostname,
		Status:   worker.Status.String(),
		CPUCores: worker.CPUCores,
		OS:       worker.OS,
		Memory:   worker.Memory,
		Storage:  worker.Storage,
	}
	writeJSON(w, http.StatusOK, wr)
}
