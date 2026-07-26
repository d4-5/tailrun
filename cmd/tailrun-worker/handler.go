package main

import (
	"encoding/json"
	"net/http"
)

type HealthResponse struct {
	Status          string  `json:"status"`
	CPUUsagePercent float64 `json:"cpuUsagePercent"`
	MemoryUsed      Bytes   `json:"memoryUsed"`
	StorageUsed     Bytes   `json:"storageUsed"`
}

type Handler struct {
	runner *Runner
}

func NewHandler(runner *Runner) *Handler {
	return &Handler{runner: runner}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /tasks", h.HandlePostTask)
	mux.HandleFunc("GET /health", h.HandleGetHealth)
}

func (h *Handler) HandlePostTask(w http.ResponseWriter, r *http.Request) {
	var task Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if task.Command == "" {
		http.Error(w, "command is required", http.StatusBadRequest)
		return
	}

	if err := h.runner.Start(task); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) HandleGetHealth(w http.ResponseWriter, r *http.Request) {
	sys, err := getSysLoad()
	if err != nil {
		http.Error(w, "failed to get system load info", http.StatusInternalServerError)
		return
	}

	running := h.runner.Status()
	status := "available"
	if running {
		status = "busy"
	}

	resp := HealthResponse{
		Status:          status,
		CPUUsagePercent: sys.CPUUsagePercent,
		MemoryUsed:      sys.MemoryUsed,
		StorageUsed:     sys.StorageUsed,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
