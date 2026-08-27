package broker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type Event struct {
	Type     string          `json:"type"`
	Data     json.RawMessage `json:"data"`
	Sequence uint64          `json:"sequence"`
}

type Broker struct {
	mu      sync.Mutex
	clients map[chan Event]struct{}
	logger  *slog.Logger
	nextSeq uint64
}

func New(logger *slog.Logger) *Broker {
	return &Broker{
		mu:      sync.Mutex{},
		clients: make(map[chan Event]struct{}),
		logger:  logger,
		nextSeq: 0,
	}
}

func (b *Broker) Publish(eventType string, data any) {
	b.mu.Lock()
	defer b.mu.Unlock()

	raw, err := json.Marshal(data)
	if err != nil {
		slog.Error("failed to marshal event data",
			"event_type", eventType,
			"error", err,
			"data_type", fmt.Sprintf("%T", data),
		)
		return
	}

	b.nextSeq++
	ev := Event{
		Type:     eventType,
		Data:     raw,
		Sequence: b.nextSeq,
	}

	for ch := range b.clients {
		select {
		case ch <- ev:
		default:
			b.logger.Error("client buffer full",
				"sequence", b.nextSeq)
		}
	}
}

func (b *Broker) Handler(w http.ResponseWriter, r *http.Request) {
	logger := b.logger.With(
		"remote_addr", r.RemoteAddr,
		"user_agent", r.UserAgent(),
	)

	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		logger.Error("failed to configure SSE streaming", "error", err)
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan Event, 50)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.clients, ch)
		b.mu.Unlock()
	}()

	if _, err := fmt.Fprintf(w, "event: connected\ndata: {}\n\n"); err != nil {
		logger.Debug("SSE client disconnected while sending connection event", "error", err)
		return
	}
	if err := rc.Flush(); err != nil {
		logger.Debug("SSE client disconnected while flushing connection event", "error", err)
		return
	}

	ctx := r.Context()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case event := <-ch:
			payload, err := json.Marshal(event)
			if err != nil {
				logger.Error("failed to marshal event", "sequence", event.Sequence, "error", err)
				continue
			}

			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				logger.Debug("SSE client disconnected while sending event",
					"sequence", event.Sequence,
					"event_type", event.Type,
					"error", err,
				)
				return
			}
			if err := rc.Flush(); err != nil {
				logger.Debug("SSE client disconnected while flushing event",
					"sequence", event.Sequence,
					"event_type", event.Type,
					"error", err,
				)
				return
			}

		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				logger.Debug("SSE client disconnected while sending heartbeat", "error", err)
				return
			}
			if err := rc.Flush(); err != nil {
				logger.Debug("SSE client disconnected while flushing heartbeat", "error", err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
