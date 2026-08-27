package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"tailscale.com/tsnet"
)

type slogWriter struct {
	logger *slog.Logger
}

func (w *slogWriter) Write(p []byte) (n int, err error) {
	message := strings.TrimSuffix(string(p), "\n")
	w.logger.Debug(message)
	return len(p), nil
}

type Bytes uint64

type RegisterWorkerRequest struct {
	URL      string `json:"url"`
	Hostname string `json:"hostname"`
	CPUCores int    `json:"cpuCores"`
	OS       string `json:"os"`
	Memory   *Bytes `json:"memory,omitempty"`
	Storage  *Bytes `json:"storage,omitempty"`
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
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, or error")
	logFormat := flag.String("log-format", "json", "Log format: json or text")
	localMode := flag.Bool("local", false, "Run in local mode without Tailscale")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level %q: %v\n", *logLevel, err)
		os.Exit(2)
	}

	options := &slog.HandlerOptions{Level: level}
	var logHandler slog.Handler
	switch *logFormat {
	case "json":
		logHandler = slog.NewJSONHandler(os.Stdout, options)
	case "text":
		logHandler = slog.NewTextHandler(os.Stdout, options)
	default:
		fmt.Fprintf(os.Stderr, "invalid log format %q: must be json or text\n", *logFormat)
		os.Exit(2)
	}
	logger := slog.New(logHandler)

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
		defer func() {
			if err := ts.Close(); err != nil {
				logger.Warn("failed to close tsnet server", "error", err)
			}
		}()

		// tsnet writes to both its Logf callback and the stdlib log package.
		// This bridges both sources to the application logger at debug level.
		log.SetOutput(&slogWriter{logger: logger})
		ts.Logf = func(format string, args ...any) {
			logger.Debug(fmt.Sprintf("tsnet: "+format, args...))
		}

		ln, err = ts.Listen("tcp", *listenAddr)
	}

	if err != nil {
		logger.Error("failed to create listener", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Warn("failed to close listener", "error", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	runner := NewRunner(ctx, *controllerURL, logger)
	h := NewHandler(runner, cancel, logger)

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

	select {
	case <-quit:
		logger.Info("shutdown triggered by signal")
	case <-ctx.Done():
		logger.Info("shutdown triggered by context cancellation")
	}

	logger.Info("tailrun-worker shutting down")

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shut down tailrun-worker server", "error", err)
	}

	logger.Info("tailrun-worker shutdown complete")
}

func registerWorker(controllerURL, workerURL string, logger *slog.Logger) error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %w", err)
	}

	var memory *Bytes
	if memoryInfo, err := mem.VirtualMemory(); err == nil {
		memoryValue := Bytes(memoryInfo.Total)
		memory = &memoryValue
	} else {
		logger.Warn("failed to get memory information", "error", err)
	}

	var storage *Bytes
	if storageInfo, err := disk.Usage("/"); err == nil {
		storageValue := Bytes(storageInfo.Total)
		storage = &storageValue
	} else {
		logger.Warn("failed to get storage information", "error", err)
	}

	reqBody := RegisterWorkerRequest{
		URL:      workerURL,
		Hostname: hostname,
		CPUCores: runtime.NumCPU(),
		OS:       runtime.GOOS,
		Memory:   memory,
		Storage:  storage,
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
	defer func() { _ = resp.Body.Close() }()

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
