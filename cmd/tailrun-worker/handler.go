package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"uuid"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
)

type HealthResponse struct {
	Status          string   `json:"status"`
	CPUUsagePercent *float64 `json:"cpuUsagePercent,omitempty"`
	MemoryUsed      *Bytes   `json:"memoryUsed,omitempty"`
	StorageUsed     *Bytes   `json:"storageUsed,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type Handler struct {
	runner *Runner
	logger *slog.Logger
}

func NewHandler(runner *Runner, logger *slog.Logger) *Handler {
	return &Handler{
		runner: runner,
		logger: logger,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /tasks", h.HandlePostTask)
	mux.HandleFunc("GET /health", h.HandleGetHealth)
}

func (h *Handler) HandlePostTask(w http.ResponseWriter, r *http.Request) {
	var task Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if task.Command == "" {
		writeError(w, http.StatusBadRequest, "command is required")
		return
	}

	if task.AttemptID == uuid.Nil() {
		writeError(w, http.StatusBadRequest, "attemptId is required")
		return
	}

	if task.DispatchSequence == 0 {
		writeError(w, http.StatusBadRequest, "invalid dispatchSequence")
		return
	}

	h.runner.Start(task)

	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) HandleGetHealth(w http.ResponseWriter, r *http.Request) {
	status := "available"
	if h.runner.Status() {
		status = "busy"
	}

	resp := HealthResponse{
		Status: status,
	}

	if percent, err := cpu.Percent(0, false); err == nil {
		resp.CPUUsagePercent = &percent[0]
	}

	if memory, err := mem.VirtualMemory(); err == nil {
		memoryUsed := Bytes(memory.Used)
		resp.MemoryUsed = &memoryUsed
	}

	if storage, err := disk.Usage("/"); err == nil {
		storageUsed := Bytes(storage.Used)
		resp.StorageUsed = &storageUsed
	}

	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	buf, err := json.Marshal(v)
	if err != nil {
		buf = []byte(`{"error":"failed to encode response"}`)
		status = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buf)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}
