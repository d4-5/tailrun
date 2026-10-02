package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/n9cw/tailrun/internal/pool"
)

type Workers interface {
	AddWorker(info pool.NewWorker) int
	GetWorkers() []pool.WorkerInfo
	GetWorker(id int) (pool.WorkerInfo, error)
	GetResourceUsage(id int) ([]pool.ResourceUsageInfo, error)
}

type RegisterWorkerRequest struct {
	Name     string      `json:"name"`
	URL      string      `json:"url"`
	Hostname string      `json:"hostname"`
	CPUCores int         `json:"cpuCores"`
	OS       string      `json:"os"`
	Memory   *pool.Bytes `json:"memory,omitempty"`
	Storage  *pool.Bytes `json:"storage,omitempty"`
}

type RegisterWorkerResponse struct {
	ID int `json:"id"`
}

type WorkerResponse struct {
	ID       int         `json:"id"`
	Name     string      `json:"name"`
	URL      string      `json:"url"`
	Hostname string      `json:"hostname"`
	Status   string      `json:"status"`
	CPUCores int         `json:"cpuCores"`
	OS       string      `json:"os"`
	Memory   *pool.Bytes `json:"memory,omitempty"`
	Storage  *pool.Bytes `json:"storage,omitempty"`
}

type ResourceUsageResponse struct {
	Timestamp       time.Time   `json:"timestamp"`
	CPUUsagePercent *float64    `json:"cpuUsagePercent,omitempty"`
	MemoryUsed      *pool.Bytes `json:"memoryUsed,omitempty"`
	StorageUsed     *pool.Bytes `json:"storageUsed,omitempty"`
}

type WorkerHandler struct {
	workers Workers
}

const maxWorkerNameCharacters = 64

func NewWorkerHandler(workers Workers) *WorkerHandler {
	return &WorkerHandler{workers: workers}
}

func (h *WorkerHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/workers", h.HandleRegisterWorker)
	mux.HandleFunc("GET /api/workers", h.HandleGetWorkers)
	mux.HandleFunc("GET /api/workers/{id}", h.HandleGetWorker)
	mux.HandleFunc("GET /api/workers/{id}/resource-usage", h.HandleGetResourceUsage)
}

func (h *WorkerHandler) HandleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req RegisterWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if utf8.RuneCountInString(req.Name) > maxWorkerNameCharacters {
		writeError(w, http.StatusBadRequest, "name must be at most 64 characters")
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
	if req.Memory != nil && *req.Memory <= 0 {
		writeError(w, http.StatusBadRequest, "memory must be greater than 0 bytes")
		return
	}
	if req.Storage != nil && *req.Storage <= 0 {
		writeError(w, http.StatusBadRequest, "storage must be greater than 0 bytes")
		return
	}

	id := h.workers.AddWorker(pool.NewWorker{
		Name:     req.Name,
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
			Name:     worker.Name,
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
		Name:     worker.Name,
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

func (h *WorkerHandler) HandleGetResourceUsage(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid worker id")
		return
	}

	usage, err := h.workers.GetResourceUsage(id)
	if err != nil {
		if errors.Is(err, pool.ErrWorkerNotFound) {
			writeError(w, http.StatusNotFound, "worker not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	resp := make([]ResourceUsageResponse, len(usage))
	for i, sample := range usage {
		resp[i] = ResourceUsageResponse{
			Timestamp:       sample.Timestamp,
			CPUUsagePercent: sample.CPUUsagePercent,
			MemoryUsed:      sample.MemoryUsed,
			StorageUsed:     sample.StorageUsed,
		}
	}

	writeJSON(w, http.StatusOK, resp)
}
