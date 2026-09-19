package socks5

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"core-proxy/pkg/common/observability"
	"core-proxy/pkg/dataplane/transport"
)

type OutboundDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type ConnHandler struct {
	conn        net.Conn
	cfg         ServerConfig
	dialer      OutboundDialer
	idleTimeout time.Duration
}

type ServerConfig struct {
	Username string
	Password string
}

func NewConnHandler(conn net.Conn, cfg ServerConfig, dialer OutboundDialer) *ConnHandler {
	return &ConnHandler{
		conn:        conn,
		cfg:         cfg,
		dialer:      dialer,
		idleTimeout: 300 * time.Second,
	}
}

func (h *ConnHandler) Handle(ctx context.Context) {
	defer h.conn.Close()

	if err := h.authenticate(); err != nil {
		observability.Warn("SOCKS5 auth failed", "client", h.conn.RemoteAddr().String(), "err", err)
		return
	}

	req, err := h.readRequest()
	if err != nil {
		observability.Warn("SOCKS5 read request failed", "client", h.conn.RemoteAddr().String(), "err", err)
		return
	}

	if req.Command != CmdConnect {
		_ = SendReply(h.conn, ReplyCommandNotSupported, nil)
		observability.Warn("Unsupported SOCKS5 command", "cmd", req.Command)
		return
	}

	targetAddr := req.Address()
	observability.Info("SOCKS5 CONNECT request", "client", h.conn.RemoteAddr().String(), "target", targetAddr)

	remoteConn, err := h.dialer.DialContext(ctx, "tcp", targetAddr)
	if err != nil {
		observability.Error("SOCKS5 dial target failed", "target", targetAddr, "err", err)
		_ = SendReply(h.conn, ReplyHostUnreachable, nil)
		return
	}
	defer remoteConn.Close()

	if err := SendReply(h.conn, ReplySuccess, remoteConn.LocalAddr()); err != nil {
		observability.Error("SOCKS5 send reply success failed", "err", err)
		return
	}

	transport.Relay(h.conn, remoteConn, transport.PipeOption{
		IdleTimeout: h.idleTimeout,
	})
}

func (h *ConnHandler) authenticate() error {
	buf := make([]byte, 2)
	if _, err := io.ReadFull(h.conn, buf); err != nil {
		return err
	}

	if buf[0] != VersionSOCKS5 {
		return fmt.Errorf("invalid SOCKS version: %d", buf[0])
	}

	numMethods := int(buf[1])
	methods := make([]byte, numMethods)
	if _, err := io.ReadFull(h.conn, methods); err != nil {
		return err
	}

	if h.cfg.Username != "" && h.cfg.Password != "" {
		if !containsMethod(methods, AuthMethodUserPass) {
			_ = h.conn.Write([]byte{VersionSOCKS5, AuthMethodNoAcceptable})
			return fmt.Errorf("user/pass auth required by server but not offered by client")
		}
		if _, err := h.conn.Write([]byte{VersionSOCKS5, AuthMethodUserPass}); err != nil {
			return err
		}
		return h.handleUserPassAuth()
	}

	if !containsMethod(methods, AuthMethodNoAuth) {
		_ = h.conn.Write([]byte{VersionSOCKS5, AuthMethodNoAcceptable})
		return fmt.Errorf("no-auth required but not offered by client")
	}

	_, err := h.conn.Write([]byte{VersionSOCKS5, AuthMethodNoAuth})
	return err
}

func (h *ConnHandler) handleUserPassAuth() error {
	header := make([]byte, 2)
	if _, err := io.ReadFull(h.conn, header); err != nil {
		return err
	}

	userLen := int(header[1])
	userBuf := make([]byte, userLen)
	if _, err := io.ReadFull(h.conn, userBuf); err != nil {
		return err
	}

	passLenBuf := make([]byte, 1)
	if _, err := io.ReadFull(h.conn, passLenBuf); err != nil {
		return err
	}

	passLen := int(passLenBuf[0])
	passBuf := make([]byte, passLen)
	if _, err := io.ReadFull(h.conn, passBuf); err != nil {
		return err
	}

	user := string(userBuf)
	pass := string(passBuf)

	if user == h.cfg.Username && pass == h.cfg.Password {
		_, err := h.conn.Write([]byte{0x01, 0x00})
		return err
	}

	_, _ = h.conn.Write([]byte{0x01, 0x01})
	return fmt.Errorf("invalid credentials")
}

func (h *ConnHandler) readRequest() (*Request, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(h.conn, header); err != nil {
		return nil, err
	}

	req := &Request{
		Version:  header[0],
		Command:  header[1],
		AddrType: header[3],
	}

	switch req.AddrType {
	case AddrTypeIPv4:
		addr := make([]byte, 4)
		if _, err := io.ReadFull(h.conn, addr); err != nil {
			return nil, err
		}
		req.DestAddr = net.IP(addr).String()
	case AddrTypeDomain:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(h.conn, lenBuf); err != nil {
			return nil, err
		}
		domainLen := int(lenBuf[0])
		domainBuf := make([]byte, domainLen)
		if _, err := io.ReadFull(h.conn, domainBuf); err != nil {
			return nil, err
		}
		req.DestAddr = string(domainBuf)
	case AddrTypeIPv6:
		addr := make([]byte, 16)
		if _, err := io.ReadFull(h.conn, addr); err != nil {
			return nil, err
		}
		req.DestAddr = net.IP(addr).String()
	default:
		return nil, fmt.Errorf("unsupported address type: %d", req.AddrType)
	}

	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(h.conn, portBuf); err != nil {
		return nil, err
	}
	req.DestPort = uint16(portBuf[0])<<8 | uint16(portBuf[1])

	return req, nil
}

func containsMethod(methods []byte, target uint8) bool {
	for _, m := range methods {
		if m == target {
			return true
		}
	}
	return false
}

