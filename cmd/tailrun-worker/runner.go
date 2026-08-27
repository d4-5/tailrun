package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"
)

type Task struct {
	ID      int               `json:"id"`
	Command string            `json:"command"`
	EnvVars map[string]string `json:"envVars"`
}

type AddTaskLogsRequest struct {
	Stdout  string `json:"stdout"`
	Stderr  string `json:"stderr"`
	Success bool   `json:"success"`
}

type Runner struct {
	mu            sync.Mutex
	running       bool
	ctx           context.Context
	controllerURL string
	httpClient    *http.Client
	logger        *slog.Logger
}

func NewRunner(ctx context.Context, controllerURL string, logger *slog.Logger) *Runner {
	return &Runner{
		ctx:           ctx,
		controllerURL: controllerURL,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		logger:        logger,
	}
}

func (r *Runner) Status() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running
}

func (r *Runner) Start(task Task) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("worker is already running task")
	}
	r.running = true
	r.mu.Unlock()

	go r.execute(task)

	return nil
}

func (r *Runner) execute(task Task) {
	r.logger.Info("starting task execution", "task_id", task.ID, "command", task.Command)

	cmd := exec.CommandContext(r.ctx, "/bin/sh", "-c", task.Command)
	cmd.Env = os.Environ()
	for k, v := range task.EnvVars {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err := cmd.Run()

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
	}

	var success bool
	if err == nil {
		success = true
	}
	stdout := stdoutBuf.String()
	stderr := stderrBuf.String()

	r.logger.Info("task execution completed", "task_id", task.ID, "success", success)

	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	go r.sendLogs(task.ID, stdout, stderr, success)
}

func (r *Runner) sendLogs(taskID int, stdout, stderr string, success bool) {
	reqBody := AddTaskLogsRequest{
		Stdout:  stdout,
		Stderr:  stderr,
		Success: success,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		r.logger.Error("failed to marshal task logs",
			"task_id", taskID,
			"error", err,
		)
		return
	}

	url := fmt.Sprintf("%s/api/tasks/%d/logs", r.controllerURL, taskID)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		r.logger.Error("failed to create log upload request",
			"task_id", taskID,
			"error", err,
		)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		r.logger.Error("failed to send logs to controller",
			"task_id", taskID,
			"error", err,
		)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			r.logger.Error("failed to read response body",
				"task_id", taskID,
				"error", err,
			)
			return
		}

		r.logger.Error("failed to send task logs to controller",
			"task_id", taskID,
			"status", resp.StatusCode,
			"response", string(respBody),
		)
	}
}
