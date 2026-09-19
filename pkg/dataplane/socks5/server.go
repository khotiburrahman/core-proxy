package socks5

import (
	"context"
	"net"
	"sync"

	"core-proxy/pkg/common/observability"
)

type Server struct {
	addr     string
	cfg      ServerConfig
	dialer   OutboundDialer
	listener net.Listener
	mu       sync.Mutex
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewServer(addr string, cfg ServerConfig, dialer OutboundDialer) *Server {
	return &Server{
		addr:   addr,
		cfg:    cfg,
		dialer: dialer,
	}
}

func (s *Server) Start(parentCtx context.Context) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.ctx, s.cancel = context.WithCancel(parentCtx)
	s.listener = listener

	observability.Info("SOCKS5 Server started listening", "addr", s.addr)

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				observability.Error("SOCKS5 accept connection error", "err", err)
				continue
			}
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			handler := NewConnHandler(c, s.cfg, s.dialer)
			handler.Handle(s.ctx)
		}(conn)
	}
}

func (s *Server) Stop() {
	s.mu.Lock()
	if s.cancel == nil {
		s.mu.Unlock()
		return
	}
	s.cancel()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	observability.Info("SOCKS5 Server stopped gracefully", "addr", s.addr)
}

