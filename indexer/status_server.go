package indexer

import (
	"context"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"go.uber.org/zap"
	"net/http"
)

type StatusServer struct {
	server *http.Server
}

func NewStatusServer(i *Indexer) *StatusServer {
	r := chi.NewRouter()
	s := &http.Server{Addr: ":3300", Handler: r}

	// setup endpoints
	r.Use(middleware.Heartbeat("/health"))
	r.Get("/stop", func(w http.ResponseWriter, r *http.Request) {
		i.StopIndexing()
		w.WriteHeader(http.StatusOK)
	})

	return &StatusServer{server: s}
}

func (s *StatusServer) Start() {
	go func() {
		err := s.server.ListenAndServe()
		if err != nil {
			zap.S().Infof("erron on StatusServer: %s", err.Error())
		}
	}()
}

func (s *StatusServer) Stop() {
	err := s.server.Shutdown(context.Background())
	if err != nil {
		zap.S().Errorf("StatusServer: %s", err.Error())
	}
}
