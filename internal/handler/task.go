package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"uuid"

	"github.com/n9cw/tailrun/internal/scheduler"
)

type Schedule interface {
	AddTask(job scheduler.NewTask) int
	GetTask(id int) (scheduler.TaskInfo, error)
	GetTasks() []scheduler.TaskInfo
	GetTaskLogs(id int) (stdout, stderr string, err error)
	AddTaskLogs(id int, stdout, stderr string, success bool, attemptID uuid.UUID) error
}

type CreateTaskRequest struct {
	Name    string            `json:"name"`
	Command string            `json:"command"`
	EnvVars map[string]string `json:"envVars,omitempty"`
}

type CreateTaskResponse struct {
	ID int `json:"id"`
}

type TaskResponse struct {
	ID            int               `json:"id"`
	Name          string            `json:"name"`
	Command       string            `json:"command"`
	Status        string            `json:"status"`
	WorkerID      *int              `json:"workerId,omitempty"`
	EnvVars       map[string]string `json:"envVars,omitempty"`
	QueuePosition *int              `json:"queuePosition,omitempty"`
}

type AddTaskLogsRequest struct {
	Stdout    string    `json:"stdout"`
	Stderr    string    `json:"stderr"`
	Success   bool      `json:"success"`
	AttemptID uuid.UUID `json:"attemptId"`
}

type TaskLogsResponse struct {
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type TaskHandler struct {
	schedule Schedule
}

func NewTaskHandler(schedule Schedule) *TaskHandler {
	return &TaskHandler{schedule: schedule}
}

func (h *TaskHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/tasks", h.HandleCreateTask)
	mux.HandleFunc("GET /api/tasks", h.HandleGetTasks)
	mux.HandleFunc("GET /api/tasks/{id}", h.HandleGetTask)
	mux.HandleFunc("POST /api/tasks/{id}/logs", h.HandleAddTaskLogs)
	mux.HandleFunc("GET /api/tasks/{id}/logs", h.HandleGetTaskLogs)
}

func (h *TaskHandler) HandleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Command == "" {
		writeError(w, http.StatusBadRequest, "command is required")
		return
	}

	id := h.schedule.AddTask(scheduler.NewTask{
		Name:    req.Name,
		Command: req.Command,
		EnvVars: req.EnvVars,
	})

	writeJSON(w, http.StatusCreated, CreateTaskResponse{ID: id})
}

func (h *TaskHandler) HandleGetTasks(w http.ResponseWriter, r *http.Request) {
	tasks := h.schedule.GetTasks()

	resp := make([]TaskResponse, 0, len(tasks))
	for _, t := range tasks {
		tr := TaskResponse{
			ID:            t.ID,
			Name:          t.Name,
			Command:       t.Command,
			Status:        t.Status.String(),
			WorkerID:      t.WorkerID,
			EnvVars:       t.EnvVars,
			QueuePosition: t.QueuePosition,
		}
		resp = append(resp, tr)
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *TaskHandler) HandleGetTask(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id")
		return
	}

	task, err := h.schedule.GetTask(id)
	if err != nil {
		if errors.Is(err, scheduler.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	tr := TaskResponse{
		ID:            task.ID,
		Name:          task.Name,
		Command:       task.Command,
		Status:        task.Status.String(),
		WorkerID:      task.WorkerID,
		EnvVars:       task.EnvVars,
		QueuePosition: task.QueuePosition,
	}
	writeJSON(w, http.StatusOK, tr)
}

func (h *TaskHandler) HandleAddTaskLogs(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id")
		return
	}

	var req AddTaskLogsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.AttemptID == uuid.Nil() {
		writeError(w, http.StatusBadRequest, "attemptId is required")
		return
	}

	if err := h.schedule.AddTaskLogs(id, req.Stdout, req.Stderr, req.Success, req.AttemptID); err != nil {
		if errors.Is(err, scheduler.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		if errors.Is(err, scheduler.ErrTaskAttemptExpired) {
			writeError(w, http.StatusConflict, "task attempt expired")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *TaskHandler) HandleGetTaskLogs(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id")
		return
	}

	stdout, stderr, err := h.schedule.GetTaskLogs(id)
	if err != nil {
		if errors.Is(err, scheduler.ErrTaskLogsNotFound) {
			writeError(w, http.StatusNotFound, "task logs not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusOK, TaskLogsResponse{Stdout: stdout, Stderr: stderr})
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
