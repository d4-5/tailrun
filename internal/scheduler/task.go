package scheduler

import (
	"fmt"
	"uuid"
)

const (
	EventTaskCreated = "task_created"
	EventTaskUpdated = "task_updated"
)

type TaskUpdatedEvent struct {
	ID       int        `json:"id"`
	Status   string     `json:"status"`
	WorkerID *uuid.UUID `json:"workerId,omitempty"`
}

type TaskCreatedEvent struct {
	ID      int               `json:"id"`
	Name    string            `json:"name"`
	Command string            `json:"command"`
	Status  string            `json:"status"`
	EnvVars map[string]string `json:"envVars,omitempty"`
}

type TaskStatus int

const (
	Waiting TaskStatus = iota
	Executing
	Finished
	Failed
)

func (s TaskStatus) String() string {
	switch s {
	case Waiting:
		return "waiting"
	case Executing:
		return "executing"
	case Finished:
		return "finished"
	case Failed:
		return "failed"
	default:
		panic(fmt.Sprintf("scheduler: unknown TaskStatus value: %d", s))
	}
}

type task struct {
	ID        int
	WorkerID  *uuid.UUID
	Name      string
	Command   string
	Status    TaskStatus
	EnvVars   map[string]string
	Stdout    string
	Stderr    string
	attemptID uuid.UUID
}

type TaskInfo struct {
	ID            int
	WorkerID      *uuid.UUID
	Name          string
	Command       string
	Status        TaskStatus
	EnvVars       map[string]string
	Stdout        string
	Stderr        string
	QueuePosition *int
}

type NewTask struct {
	Name    string
	Command string
	EnvVars map[string]string
}
