package scheduler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/n9cw/tailrun/internal/pool"
)

const channelBufferSize = 50

type Pool interface {
	AvailableWorker() <-chan pool.Worker
	WorkerError() <-chan pool.WorkerError
	ReleaseWorker(id int)
}

type Broker interface {
	Publish(eventType string, data any)
}

var (
	ErrTaskNotFound     = errors.New("task not found")
	ErrTaskLogsNotFound = errors.New("task logs not found")
)

type addTaskReq struct {
	newTask NewTask
	reply   chan int
}

type getTasksReq struct {
	reply chan []TaskInfo
}

type getTaskReq struct {
	id    int
	reply chan getTaskResult
}

type getTaskResult struct {
	task TaskInfo
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
	workerErrorCh    <-chan pool.WorkerError
	waitingForWorker bool
	broker           Broker
}

func New(pool Pool, broker Broker, logger *slog.Logger) *Scheduler {
	s := &Scheduler{
		pool:             pool,
		logger:           logger,
		addTaskCh:        make(chan addTaskReq, channelBufferSize),
		getTasksCh:       make(chan getTasksReq, channelBufferSize),
		getTaskCh:        make(chan getTaskReq, channelBufferSize),
		getTaskLogsCh:    make(chan getTaskLogsReq, channelBufferSize),
		addTaskLogsCh:    make(chan addTaskLogsReq, channelBufferSize),
		workerReadyCh:    make(chan workerReady, channelBufferSize),
		dispatchResultCh: make(chan dispatchResult, channelBufferSize),
		workerErrorCh:    pool.WorkerError(),
		tasks:            make(map[int]*Task),
		queue:            make([]int, 0),
		nextID:           1,
		broker:           broker,
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
		case r := <-s.workerErrorCh:
			s.handleWorkerError(r)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Scheduler) handleWorkerError(r pool.WorkerError) {
	if !errors.Is(r.Err, pool.ErrWorkerDied) {
		s.logger.Warn(
			"received unexpected worker error",
			"worker_id", r.WorkerID,
			"error", r.Err,
		)
		return
	}

	s.logger.Warn("worker died",
		"worker_id", r.WorkerID,
		"error", r.Err,
	)

	for _, task := range s.tasks {
		if task.Status != Executing || task.WorkerID == nil || *task.WorkerID != r.WorkerID {
			continue
		}

		task.Status = Waiting
		task.WorkerID = nil
		s.queue = append([]int{task.ID}, s.queue...)

		s.broker.Publish(EventTaskUpdated, TaskUpdatedEvent{
			ID:       task.ID,
			Status:   task.Status.String(),
			WorkerID: task.WorkerID,
		})
		s.tryRequestWorker()
		return
	}

	s.logger.Warn("worker died without an executing task", "worker_id", r.WorkerID)
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

	s.broker.Publish(EventTaskCreated, TaskCreatedEvent{
		ID:      id,
		Name:    task.Name,
		Command: task.Command,
		EnvVars: task.EnvVars,
		Status:  task.Status.String(),
	})
	r.reply <- id

	s.tryRequestWorker()
}

func (s *Scheduler) handleGetTasks(r getTasksReq) {
	positionMap := make(map[int]int, len(s.queue))
	for i, id := range s.queue {
		positionMap[id] = i + 1
	}

	tasks := make([]TaskInfo, 0, len(s.tasks))
	for _, t := range s.tasks {
		info := TaskInfo{
			ID:       t.ID,
			WorkerID: t.WorkerID,
			Name:     t.Name,
			Command:  t.Command,
			Status:   t.Status,
			EnvVars:  t.EnvVars,
			Stdout:   t.Stdout,
			Stderr:   t.Stderr,
		}
		if t.Status == Waiting {
			if pos, ok := positionMap[t.ID]; ok {
				info.QueuePosition = &pos
			} else {
				s.logger.Warn("task has Waiting status but not in queue",
					"task_id", t.ID)
			}
		}
		tasks = append(tasks, info)
	}
	r.reply <- tasks
}

func (s *Scheduler) handleGetTask(r getTaskReq) {
	t, ok := s.tasks[r.id]
	if !ok {
		r.reply <- getTaskResult{err: ErrTaskNotFound}
		return
	}

	info := TaskInfo{
		ID:       t.ID,
		WorkerID: t.WorkerID,
		Name:     t.Name,
		Command:  t.Command,
		Status:   t.Status,
		EnvVars:  t.EnvVars,
		Stdout:   t.Stdout,
		Stderr:   t.Stderr,
	}

	if t.Status == Waiting {
		for i, id := range s.queue {
			if id == r.id {
				pos := i + 1
				info.QueuePosition = &pos
				break
			}
		}
	}

	r.reply <- getTaskResult{task: info}
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

	s.broker.Publish(EventTaskUpdated, TaskUpdatedEvent{
		ID:       t.ID,
		Status:   t.Status.String(),
		WorkerID: t.WorkerID,
	})

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
			s.broker.Publish(EventTaskUpdated, TaskUpdatedEvent{
				ID:       t.ID,
				Status:   t.Status.String(),
				WorkerID: t.WorkerID,
			})
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
	s.broker.Publish(EventTaskUpdated, TaskUpdatedEvent{
		ID:       task.ID,
		Status:   task.Status.String(),
		WorkerID: task.WorkerID,
	})

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

func (s *Scheduler) GetTasks() []TaskInfo {
	reply := make(chan []TaskInfo, 1)
	s.getTasksCh <- getTasksReq{reply: reply}
	return <-reply
}

func (s *Scheduler) GetTask(id int) (TaskInfo, error) {
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
