package httpserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"smart-warehouse/internal/domain"
	"smart-warehouse/internal/infrastructure/kafka"
	"smart-warehouse/internal/infrastructure/metrics"
)

type HealthChecker interface {
	Ping(context.Context) error
}

type Server struct {
	server *http.Server
}

func New(addr string, logger *slog.Logger, registry *metrics.Registry, cassandra HealthChecker, kafkaHealth HealthChecker, producer *kafka.Producer) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", registry.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if cassandra != nil {
			if err := cassandra.Ping(ctx); err != nil {
				http.Error(w, "cassandra unavailable: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		if kafkaHealth != nil {
			if err := kafkaHealth.Ping(ctx); err != nil {
				http.Error(w, "kafka unavailable: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})
	if producer != nil {
		mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			var event domain.WarehouseEvent
			if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := producer.Publish(r.Context(), event); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status":   "accepted",
				"event_id": event.EventID,
			})
		})
	}
	return &Server{server: &http.Server{Addr: addr, Handler: logRequest(logger, registry, mux)}}
}

func (s *Server) Run() error {
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func logRequest(logger *slog.Logger, registry *metrics.Registry, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		registry.ObserveHTTPRequest(r.Method, r.URL.Path, recorder.status, time.Since(start).Seconds())
		logger.Debug("http request", "method", r.Method, "path", r.URL.Path, "status", recorder.status)
	})
}
