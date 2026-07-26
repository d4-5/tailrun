package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"tailscale.com/tsnet"
)

type Bytes uint64

type RegisterWorkerRequest struct {
	URL      string `json:"url"`
	Hostname string `json:"hostname"`
	CPUCores int    `json:"cpuCores"`
	OS       string `json:"os"`
	Memory   Bytes  `json:"memory"`
	Storage  Bytes  `json:"storage"`
}

type RegisterWorkerResponse struct {
	ID int `json:"id"`
}

func main() {
	authKey := flag.String("auth-key", "", "Tailscale auth key used to join the tailnet")
	controllerURL := flag.String("controller-url", "", "Controller URL")
	homeDir, _ := os.UserHomeDir()
	defaultTsnetDir := filepath.Join(homeDir, ".local", "tailrun-worker")
	tsnetDir := flag.String("tsnet-dir", defaultTsnetDir, "Directory for tsnet state")
	listenAddr := flag.String("listen", ":80", "Listen address")
	tsnetLogs := flag.Bool("tsnet-logs", false, "Enable tsnet logs")
	localMode := flag.Bool("local", false, "Run in local mode without Tailscale")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if !*localMode && *authKey == "" {
		logger.Error("failed to start", "error", errors.New("missing required auth key"))
		os.Exit(1)
	}

	if *controllerURL == "" {
		logger.Error("failed to start", "error", errors.New("missing required controller URL"))
		os.Exit(1)
	}

	var ln net.Listener
	var err error

	if *localMode {
		ln, err = net.Listen("tcp", *listenAddr)
	} else {
		ts := &tsnet.Server{
			AuthKey: *authKey,
			Dir:     *tsnetDir,
		}
		defer ts.Close()

		if *tsnetLogs {
			ts.Logf = func(format string, args ...any) {
				logger.Info(fmt.Sprintf("tsnet: "+format, args...))
			}
		}

		ln, err = ts.Listen("tcp", *listenAddr)
	}

	if err != nil {
		logger.Error("failed to create listener", "error", err)
		os.Exit(1)
	}
	defer ln.Close()

	runner := NewRunner(*controllerURL, logger)
	h := NewHandler(runner)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	server := &http.Server{
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("failed to start http server", "error", err)
		}
	}()

	workerURL := fmt.Sprintf("http://%s", ln.Addr().String())
	err = registerWorker(*controllerURL, workerURL, logger)
	if err != nil {
		logger.Error("failed to register tailrun-worker", "error", err)
		os.Exit(1)
	}

	logger.Info("tailrun-worker started", "address", ln.Addr().String())

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("tailrun-worker shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shut down tailrun-worker server", "error", err)
	}

	logger.Info("tailrun-worker shutdown complete")
}

func registerWorker(controllerURL, workerURL string, logger *slog.Logger) error {
	sys, err := getSysInfo()
	if err != nil {
		return fmt.Errorf("failed to get system info: %w", err)
	}

	reqBody := RegisterWorkerRequest{
		URL:      workerURL,
		Hostname: sys.Hostname,
		CPUCores: sys.CPUCores,
		OS:       sys.OS,
		Memory:   sys.Memory,
		Storage:  sys.Storage,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal registration request: %w", err)
	}

	logger.Info("registering worker with controller", "controller", controllerURL, "url", workerURL)

	req, err := http.NewRequest(http.MethodPost, controllerURL+"/api/workers", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create registration request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("registration request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		var respData RegisterWorkerResponse
		if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
			return fmt.Errorf("failed to decode registration response: %w", err)
		}
		logger.Info("successfully registered worker", "worker_id", respData.ID)
		return nil
	}

	return fmt.Errorf("registration failed with status %d", resp.StatusCode)
}
