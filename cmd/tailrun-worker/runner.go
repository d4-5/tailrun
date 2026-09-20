package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"time"
	"uuid"
)

const (
	logRetryInitialDelay = 500 * time.Millisecond
	logRetryMaxDelay     = 30 * time.Second
)

type Task struct {
	ID               int               `json:"id"`
	Command          string            `json:"command"`
	EnvVars          map[string]string `json:"envVars"`
	AttemptID        uuid.UUID         `json:"attemptId"`
	DispatchSequence uint64            `json:"dispatchSequence"`
}

type AddTaskLogsRequest struct {
	Stdout    string    `json:"stdout"`
	Stderr    string    `json:"stderr"`
	Success   bool      `json:"success"`
	AttemptID uuid.UUID `json:"attemptId"`
}

type startTaskRequest struct {
	task Task
}

type statusRequest struct {
	reply chan bool
}

type executionResult struct {
	task      Task
	stdout    string
	stderr    string
	success   bool
	cancelled bool
}

type Runner struct {
	startTaskCh   chan startTaskRequest
	statusCh      chan statusRequest
	completionCh  chan executionResult
	activeTask    *Task
	cancelActive  context.CancelFunc
	pendingTask   *Task
	lastSequence  uint64
	controllerURL string
	httpClient    *http.Client
	logger        *slog.Logger
}

func NewRunner(controllerURL string, logger *slog.Logger) *Runner {
	return &Runner{
		startTaskCh:   make(chan startTaskRequest, 50),
		statusCh:      make(chan statusRequest, 10),
		completionCh:  make(chan executionResult, 1),
		controllerURL: controllerURL,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		logger:        logger,
	}
}

func (r *Runner) Status() bool {
	reply := make(chan bool, 1)
	r.statusCh <- statusRequest{reply: reply}
	return <-reply
}

func (r *Runner) Start(task Task) {
	r.startTaskCh <- startTaskRequest{task: task}
}

func (r *Runner) Run() {
	for {
		select {
		case req := <-r.startTaskCh:
			r.handleStartTask(req.task)

		case req := <-r.statusCh:
			req.reply <- r.activeTask != nil

		case result := <-r.completionCh:
			r.handleCompletion(result)
		}
	}
}

func (r *Runner) handleStartTask(task Task) {
	if task.DispatchSequence <= r.lastSequence {
		r.logger.Debug("ignoring stale task dispatch",
			"task_id", task.ID,
			"attempt_id", task.AttemptID,
			"dispatch_sequence", task.DispatchSequence,
			"last_dispatch_sequence", r.lastSequence,
		)
		return
	}
	r.lastSequence = task.DispatchSequence

	if r.activeTask == nil {
		r.startTask(task)
		return
	}

	r.logger.Info("cancelling previous task",
		"task_id", r.activeTask.ID,
		"attempt_id", r.activeTask.AttemptID,
	)
	r.cancelActive()
	r.pendingTask = &task
}

func (r *Runner) startTask(task Task) {
	ctx, cancel := context.WithCancel(context.Background())
	r.activeTask = &task
	r.cancelActive = cancel
	go r.execute(ctx, task)
}

func (r *Runner) handleCompletion(result executionResult) {
	if r.activeTask == nil {
		r.logger.Warn("ignoring task completion with no active task",
			"task_id", result.task.ID,
			"dispatch_sequence", result.task.DispatchSequence,
		)
		return
	}

	if result.task.DispatchSequence != r.activeTask.DispatchSequence {
		return
	}

	r.activeTask = nil
	r.cancelActive()
	r.cancelActive = nil

	if result.cancelled {
		r.logger.Info("task execution cancelled", "task_id", result.task.ID)
	} else {
		r.logger.Info("task execution completed",
			"task_id", result.task.ID,
			"success", result.success,
		)
		go r.sendLogs(
			result.task.ID,
			result.task.AttemptID,
			result.stdout,
			result.stderr,
			result.success,
		)
	}

	if r.pendingTask == nil {
		return
	}

	next := *r.pendingTask
	r.pendingTask = nil
	r.startTask(next)
}

func (r *Runner) execute(ctx context.Context, task Task) {
	r.logger.Info("starting task execution", "task_id", task.ID, "command", task.Command)

	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", task.Command)
	cmd.Env = os.Environ()
	for k, v := range task.EnvVars {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	r.completionCh <- executionResult{
		task:      task,
		stdout:    stdoutBuf.String(),
		stderr:    stderrBuf.String(),
		success:   err == nil,
		cancelled: ctx.Err() != nil,
	}
}

func (r *Runner) sendLogs(taskID int, attemptID uuid.UUID, stdout, stderr string, success bool) {
	reqBody := AddTaskLogsRequest{
		Stdout:    stdout,
		Stderr:    stderr,
		Success:   success,
		AttemptID: attemptID,
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

	delay := logRetryInitialDelay
	for attempt := 1; ; attempt++ {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			r.logger.Error("failed to create log upload request",
				"task_id", taskID,
				"error", err,
			)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		delayWithJitter := delay + time.Duration(rand.Int64N(int64(delay/5)+1))
		if delayWithJitter > logRetryMaxDelay {
			delayWithJitter = logRetryInitialDelay
		}

		resp, err := r.httpClient.Do(req)
		if err == nil {
			if resp.StatusCode == http.StatusNoContent {
				_ = resp.Body.Close()
				return
			}

			respBody, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			switch {
			case resp.StatusCode == http.StatusRequestTimeout ||
				resp.StatusCode == http.StatusTooManyRequests ||
				(resp.StatusCode >= http.StatusInternalServerError && resp.StatusCode <= 599):
				r.logger.Warn("failed to send task logs, retrying",
					"task_id", taskID,
					"attempt", attempt,
					"status", resp.StatusCode,
					"response", string(respBody),
					"read_error", readErr,
					"retry_in", delayWithJitter,
				)
			default:
				r.logger.Error("failed to send task logs to controller",
					"task_id", taskID,
					"status", resp.StatusCode,
					"response", string(respBody),
					"read_error", readErr,
				)
				return
			}
		} else {
			r.logger.Warn("failed to send task logs, retrying",
				"task_id", taskID,
				"attempt", attempt,
				"error", err,
				"retry_in", delayWithJitter,
			)
		}

		time.Sleep(delayWithJitter)
		if delay < logRetryMaxDelay {
			delay *= 2
			if delay > logRetryMaxDelay {
				delay = logRetryMaxDelay
			}
		}
	}
}
