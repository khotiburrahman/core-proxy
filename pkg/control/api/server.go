package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"core-proxy/pkg/common/observability"
	"core-proxy/pkg/control/health"
	"core-proxy/pkg/control/worker"
)

type ReloadFunc func() error

type Server struct {
	addr      string
	healthMgr *health.Manager
	workerMgr *worker.Manager
	reloadFn  ReloadFunc
	httpSrv   *http.Server
}

func NewServer(addr string, healthMgr *health.Manager, workerMgr *worker.Manager, reloadFn ReloadFunc) *Server {
	s := &Server{
		addr:      addr,
		healthMgr: healthMgr,
		workerMgr: workerMgr,
		reloadFn:  reloadFn,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/api/v1/reload", s.handleReload)
	mux.HandleFunc("/api/v1/workers", s.handleWorkers)

	s.httpSrv = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return s
}

func (s *Server) Start() error {
	observability.Info("Control Plane API listening", "addr", s.addr)
	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			observability.Error("Control Plane API error", "err", err)
		}
	}()
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"UP"}`))
}

func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := s.reloadFn(); err != nil {
		observability.Error("Hot reload failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"SUCCESS","message":"Config reloaded"}`))
}

func (s *Server) handleWorkers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "OK",
	})
}
