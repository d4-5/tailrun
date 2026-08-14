package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type CreateTaskRequest struct {
	Name    string            `json:"name"`
	Command string            `json:"command"`
	EnvVars map[string]string `json:"envVars,omitempty"`
}

var taskTemplates = []struct {
	Name string
	Cmd  string
}{
	{"Calculate Fibonacci 15", "echo 'Calculating Fibonacci 15...'; f() { if [ $1 -le 1 ]; then echo $1; else echo $(( $(f $(( $1 - 1 ))) + $(f $(( $1 - 2 ))) )); fi; }; f 15"},
	{"Simulate Web Build", "echo 'Installing packages...'; sleep 1; echo 'Bundling assets...'; sleep 2; echo 'Build successful!'; exit 0"},
	{"Compile System Logs", "echo 'Reading kernel logs...'; sleep 1; echo 'System normal.'; exit 0"},
	{"Disk Clean-up Simulation", "echo 'Checking disk utilization...'; sleep 1; df -h; echo 'Cleaning cache...'; sleep 1; echo 'Clean-up complete.'"},
	{"Process Database Migration", "echo 'Running db migration version 241...'; sleep 2; echo 'Index created successfully.'; exit 0"},
	{"Intermittent Error Job", "echo 'Connecting to remote endpoint...'; sleep 1; echo 'Error: Connection reset by peer!'; exit 1"},
}

func main() {
	fmt.Println("=== Starting E2E Demo Runner Setup ===")
	tempDir, err := os.MkdirTemp("", "tailrun-demo-*")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	controllerBin := filepath.Join(tempDir, "tailrund")
	workerBin := filepath.Join(tempDir, "tailrun-worker")

	fmt.Println("Compiling tailrund...")
	cmdBuildCtrl := exec.Command("go", "build", "-o", controllerBin, "./cmd/tailrund")
	if out, err := cmdBuildCtrl.CombinedOutput(); err != nil {
		fmt.Printf("Failed to compile tailrund: %v\nOutput: %s\n", err, out)
		os.Exit(1)
	}

	fmt.Println("Compiling tailrun-worker...")
	cmdBuildWrk := exec.Command("go", "build", "-o", workerBin, "./cmd/tailrun-worker")
	if out, err := cmdBuildWrk.CombinedOutput(); err != nil {
		fmt.Printf("Failed to compile tailrun-worker: %v\nOutput: %s\n", err, out)
		os.Exit(1)
	}

	var pids []int
	var pidsMu sync.Mutex

	startProcess := func(name string, path string, args []string) *exec.Cmd {
		cmd := exec.Command(path, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Start()
		if err != nil {
			fmt.Printf("Failed to start %s: %v\n", name, err)
			os.Exit(1)
		}
		pidsMu.Lock()
		pids = append(pids, cmd.Process.Pid)
		pidsMu.Unlock()
		fmt.Printf("Started %s (PID %d)\n", name, cmd.Process.Pid)
		return cmd
	}

	startProcess("tailrund", controllerBin, []string{"-local", "-listen", ":8080"})
	time.Sleep(1 * time.Second)

	startProcess("worker-1", workerBin, []string{"-local", "-listen", ":8081", "-controller-url", "http://localhost:8080"})
	startProcess("worker-2", workerBin, []string{"-local", "-listen", ":8082", "-controller-url", "http://localhost:8080"})
	startProcess("worker-3", workerBin, []string{"-local", "-listen", ":8083", "-controller-url", "http://localhost:8080"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			select {
			case <-ctx.Done():
				return
			default:
				tpl := taskTemplates[rng.Intn(len(taskTemplates))]
				reqBody := CreateTaskRequest{
					Name:    tpl.Name,
					Command: tpl.Cmd,
				}
				body, _ := json.Marshal(reqBody)

				fmt.Printf("[Generator] Dispatching task: %s\n", tpl.Name)
				resp, err := client.Post("http://localhost:8080/api/tasks", "application/json", bytes.NewReader(body))
				if err != nil {
					fmt.Printf("[Generator] Error submitting task: %v\n", err)
				} else {
					_ = resp.Body.Close()
				}

				sleepDuration := time.Duration(1+rng.Intn(5)) * 100 * time.Millisecond
				time.Sleep(sleepDuration)
			}
		}
	}()

	fmt.Println("\n=======================================================")
	fmt.Println("Demo is running!")
	fmt.Println("Open your browser and navigate to: http://localhost:8080")
	fmt.Println("Press Ctrl+C to terminate.")
	fmt.Println("=======================================================")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nStopping all services...")
	cancel()

	pidsMu.Lock()
	for _, pid := range pids {
		fmt.Printf("Killing process with PID %d...\n", pid)
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
	pidsMu.Unlock()

	fmt.Println("All services stopped.")
}
