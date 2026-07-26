package scheduler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/n9cw/tailrun/internal/pool"
)

type Pool interface {
	AvailableWorker() <-chan pool.Worker
	ReleaseWorker(id int)
}

var (
	ErrTaskNotFound     = errors.New("task not found")
	ErrTaskLogsNotFound = errors.New("task logs not found")
)

type TaskStatus int

const (
	Waiting TaskStatus = iota
	Executing
	Finished
	Failed
)

func (s TaskStatus) String() (string, error) {
	switch s {
	case Waiting:
		return "waiting", nil
	case Executing:
		return "executing", nil
	case Finished:
		return "finished", nil
	case Failed:
		return "failed", nil
	default:
		return "", errors.New("unknown task status")
	}
}

type Task struct {
	ID       int
	WorkerID *int
	Name     string
	Command  string
	Status   TaskStatus
	EnvVars  map[string]string
	Stdout   string
	Stderr   string
}

type NewTask struct {
	Name    string
	Command string
	EnvVars map[string]string
}

type addTaskReq struct {
	newTask NewTask
	reply   chan int
}

type getTasksReq struct {
	reply chan []Task
}

type getTaskReq struct {
	id    int
	reply chan getTaskResult
}

type getTaskResult struct {
	task Task
	err  error
}

type getTaskLogsReq struct {
	id    int
	reply chan getTaskLogsResult
}

type getTaskLogsResult struct {
	stdout string
	stderr string
	err    error
}

type addTaskLogsReq struct {
	id      int
	stdout  string
	stderr  string
	success bool
	reply   chan error
}

type workerReady struct {
	worker pool.Worker
}

type dispatchResult struct {
	taskID   int
	workerID int
	err      error
}

type Scheduler struct {
	tasks            map[int]*Task
	queue            []int
	pool             Pool
	logger           *slog.Logger
	nextID           int
	addTaskCh        chan addTaskReq
	getTasksCh       chan getTasksReq
	getTaskCh        chan getTaskReq
	getTaskLogsCh    chan getTaskLogsReq
	addTaskLogsCh    chan addTaskLogsReq
	workerReadyCh    chan workerReady
	dispatchResultCh chan dispatchResult
	waitingForWorker bool
}

func New(pool Pool, logger *slog.Logger) *Scheduler {
	s := &Scheduler{
		pool:             pool,
		logger:           logger,
		addTaskCh:        make(chan addTaskReq, 50),
		getTasksCh:       make(chan getTasksReq, 50),
		getTaskCh:        make(chan getTaskReq, 50),
		getTaskLogsCh:    make(chan getTaskLogsReq, 50),
		addTaskLogsCh:    make(chan addTaskLogsReq, 50),
		workerReadyCh:    make(chan workerReady, 50),
		dispatchResultCh: make(chan dispatchResult, 50),
		tasks:            make(map[int]*Task),
		queue:            make([]int, 0),
		nextID:           1,
	}
	return s
}

func (s *Scheduler) Run(ctx context.Context) {
	for {
		select {
		case r := <-s.addTaskCh:
			s.handleAddTask(r)
		case r := <-s.getTasksCh:
			s.handleGetTasks(r)
		case r := <-s.getTaskCh:
			s.handleGetTask(r)
		case r := <-s.getTaskLogsCh:
			s.handleGetTaskLogs(r)
		case r := <-s.addTaskLogsCh:
			s.handleAddTaskLogs(r)
		case r := <-s.workerReadyCh:
			s.handleWorkerReady(r)
		case r := <-s.dispatchResultCh:
			s.handleDispatchResult(r)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Scheduler) handleAddTask(r addTaskReq) {
	id := s.nextID
	s.nextID++
	task := &Task{
		ID:      id,
		Name:    r.newTask.Name,
		Command: r.newTask.Command,
		EnvVars: r.newTask.EnvVars,
		Status:  Waiting,
	}
	s.tasks[id] = task
	s.queue = append(s.queue, id)
	r.reply <- id

	s.tryRequestWorker()
}

func (s *Scheduler) handleGetTasks(r getTasksReq) {
	tasks := make([]Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		tasks = append(tasks, *t)
	}
	r.reply <- tasks
}

func (s *Scheduler) handleGetTask(r getTaskReq) {
	t, ok := s.tasks[r.id]
	if !ok {
		r.reply <- getTaskResult{err: ErrTaskNotFound}
		return
	}
	r.reply <- getTaskResult{task: *t}
}

func (s *Scheduler) handleGetTaskLogs(r getTaskLogsReq) {
	t, ok := s.tasks[r.id]
	if !ok {
		r.reply <- getTaskLogsResult{err: ErrTaskNotFound}
		return
	}
	r.reply <- getTaskLogsResult{stdout: t.Stdout, stderr: t.Stderr}
}

func (s *Scheduler) handleAddTaskLogs(r addTaskLogsReq) {
	t, ok := s.tasks[r.id]
	if !ok {
		r.reply <- ErrTaskNotFound
		return
	}
	t.Stdout = r.stdout
	t.Stderr = r.stderr
	if r.success {
		t.Status = Finished
	} else {
		t.Status = Failed
	}

	if t.WorkerID != nil {
		s.pool.ReleaseWorker(*t.WorkerID)
	} else {
		s.logger.Warn("task completed with no worker assigned", "task_id", r.id)
	}
	r.reply <- nil

	s.tryRequestWorker()
}

func (s *Scheduler) handleWorkerReady(r workerReady) {
	s.waitingForWorker = false
	s.dispatchTask(r.worker)
	s.tryRequestWorker()
}

func (s *Scheduler) handleDispatchResult(r dispatchResult) {
	if r.err != nil {
		s.logger.Error(
			"failed to dispatch task",
			"task_id", r.taskID,
			"worker_id", r.workerID,
			"error", r.err,
		)

		t, ok := s.tasks[r.taskID]
		if ok {
			t.Status = Waiting
			t.WorkerID = nil
			s.queue = append(s.queue, r.taskID)
		} else {
			s.logger.Warn("failed to find task for dispatch result", "task_id", r.taskID)
		}

		s.pool.ReleaseWorker(r.workerID)
	}
	s.tryRequestWorker()
}

func (s *Scheduler) tryRequestWorker() {
	if len(s.queue) == 0 || s.waitingForWorker {
		return
	}

	s.waitingForWorker = true
	go func() {
		ch := s.pool.AvailableWorker()
		w := <-ch
		s.workerReadyCh <- workerReady{worker: w}
	}()
}

func (s *Scheduler) dispatchTask(w pool.Worker) {
	if len(s.queue) == 0 {
		s.logger.Error("queue empty but worker available", "worker_id", w.ID())
		s.pool.ReleaseWorker(w.ID())
		return
	}

	id := s.queue[0]
	s.queue = s.queue[1:]

	task, ok := s.tasks[id]
	if !ok {
		s.logger.Error("failed to find task in queue", "task_id", id)
		s.pool.ReleaseWorker(w.ID())
		return
	}

	workerID := w.ID()
	task.Status = Executing
	task.WorkerID = &workerID

	taskInfo := pool.Task{
		ID:      task.ID,
		Command: task.Command,
		EnvVars: task.EnvVars,
	}

	go func() {
		err := w.Execute(taskInfo)
		s.dispatchResultCh <- dispatchResult{
			taskID:   task.ID,
			workerID: workerID,
			err:      err,
		}
	}()
}

func (s *Scheduler) AddTask(newTask NewTask) int {
	reply := make(chan int, 1)
	s.addTaskCh <- addTaskReq{newTask: newTask, reply: reply}
	return <-reply
}

func (s *Scheduler) GetTasks() []Task {
	reply := make(chan []Task, 1)
	s.getTasksCh <- getTasksReq{reply: reply}
	return <-reply
}

func (s *Scheduler) GetTask(id int) (Task, error) {
	reply := make(chan getTaskResult, 1)
	s.getTaskCh <- getTaskReq{id: id, reply: reply}
	res := <-reply
	return res.task, res.err
}

func (s *Scheduler) GetTaskLogs(id int) (stdout, stderr string, err error) {
	reply := make(chan getTaskLogsResult, 1)
	s.getTaskLogsCh <- getTaskLogsReq{id: id, reply: reply}
	res := <-reply
	return res.stdout, res.stderr, res.err
}

func (s *Scheduler) AddTaskLogs(id int, stdout, stderr string, success bool) error {
	reply := make(chan error, 1)
	s.addTaskLogsCh <- addTaskLogsReq{id: id, stdout: stdout, stderr: stderr, success: success, reply: reply}
	return <-reply
}
