package main

import (
	"context"
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
	w.logger.Debug(message)
	return len(p), nil
}

func main() {
	authKey := flag.String("auth-key", "", "Tailscale auth key used to join the tailnet")
	hostname := flag.String("hostname", "tailrund", "Tailscale hostname for this service")
	homeDir, _ := os.UserHomeDir()
	defaultTsnetDir := filepath.Join(homeDir, ".local", "tailrund")
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
		defer func() {
			if err := ts.Close(); err != nil {
				logger.Warn("failed to close tsnet server", "error", err)
			}
		}()

		// tsnet writes to both its Logf callback and the stdlib log package.
		// This dridges both sources to the application logger at debug level.
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

	server := &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
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

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shut down tailrund server", "error", err)
	}

	cancel()

	time.Sleep(100 * time.Millisecond)
	logger.Info("tailrund shutdown complete")
}
