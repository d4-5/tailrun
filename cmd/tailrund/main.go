package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"tailscale.com/tsnet"

	"github.com/n9cw/tailrun/internal/broker"
	"github.com/n9cw/tailrun/internal/handler"
	"github.com/n9cw/tailrun/internal/pool"
	"github.com/n9cw/tailrun/internal/scheduler"
	"github.com/n9cw/tailrun/internal/web"
)

type slogWriter struct {
	logger *slog.Logger
}

func (w *slogWriter) Write(p []byte) (n int, err error) {
	message := strings.TrimSuffix(string(p), "\n")
	w.logger.Info(message)
	return len(p), nil
}

func main() {
	authKey := flag.String("auth-key", "", "Tailscale auth key used to join the tailnet")
	hostname := flag.String("hostname", "tailrund", "Tailscale hostname for this service")
	homeDir, _ := os.UserHomeDir()
	defaultTsnetDir := filepath.Join(homeDir, ".local", "tailrund")
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

	eb := broker.New(logger)
	p := pool.New(eb, logger)
	s := scheduler.New(p, eb, logger)

	taskHandler := handler.NewTaskHandler(s)
	workerHandler := handler.NewWorkerHandler(p)
	staticHandler := handler.NewStaticHandler(web.FS)

	mux := http.NewServeMux()
	staticHandler.RegisterRoutes(mux)
	taskHandler.RegisterRoutes(mux)
	workerHandler.RegisterRoutes(mux)
	mux.HandleFunc("GET /api/events", eb.Handler)

	var ln net.Listener
	var err error

	if *localMode {
		ln, err = net.Listen("tcp", *listenAddr)
	} else {
		ts := &tsnet.Server{
			AuthKey:  *authKey,
			Hostname: *hostname,
			Dir:      *tsnetDir,
		}
		defer func() { _ = ts.Close() }()

		if *tsnetLogs {
			// tsnet writes to the stdlib log package
			// This bridges those logs to our slog logger
			log.SetOutput(&slogWriter{logger: logger})
			ts.Logf = func(format string, args ...any) {
				logger.Info(fmt.Sprintf("tsnet: "+format, args...))
			}
		} else {
			log.SetOutput(io.Discard)
		}

		ln, err = ts.Listen("tcp", *listenAddr)
	}

	if err != nil {
		logger.Error("failed to create listener", "error", err)
		os.Exit(1)
	}
	defer func() { _ = ln.Close() }()

	server := &http.Server{
		Handler:     mux,
		ReadTimeout: 5 * time.Second,
		// WriteTimeout must be > broker's heartbeat ticker interval
		// to prevent the server from timing out long-lived SSE connections.
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  5 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)
	go s.Run(ctx)

	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("failed to start http server", "error", err)
		}
	}()

	logger.Info("tailrund started", "address", ln.Addr().String())

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("tailrund shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shut down tailrund server", "error", err)
	}

	cancel()

	time.Sleep(100 * time.Millisecond)
	logger.Info("tailrund shutdown complete")
}
