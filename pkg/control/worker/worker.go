package worker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"

	"core-proxy/pkg/common/observability"
	"core-proxy/pkg/config"
	"core-proxy/pkg/control/health"
	"core-proxy/pkg/control/payload"
)

type SSHWorker struct {
	cfg         config.WorkerConfig
	injector    *payload.Injector
	state       atomic.Int32
	client      atomic.Pointer[ssh.Client]
	conn        atomic.Pointer[net.Conn]
	activeConns atomic.Int64
	latencyMs   atomic.Int64
	healthMgr   *health.Manager
	mu          sync.Mutex
	cancelLoop  context.CancelFunc
	wg          sync.WaitGroup
}

func NewSSHWorker(cfg config.WorkerConfig, healthMgr *health.Manager) *SSHWorker {
	w := &SSHWorker{
		cfg:       cfg,
		healthMgr: healthMgr,
	}
	if cfg.PayloadData != "" {
		inj, err := payload.NewInjector(cfg.PayloadData, 50*time.Millisecond)
		if err != nil {
			observability.Error("Failed to init payload injector", "worker_id", cfg.ID, "err", err)
		} else {
			w.injector = inj
		}
	}
	w.state.Store(int32(StateCreated))
	return w
}

func (w *SSHWorker) ID() string {
	return w.cfg.ID
}

func (w *SSHWorker) State() State {
	return State(w.state.Load())
}

func (w *SSHWorker) ActiveConnections() int64 {
	return w.activeConns.Load()
}

func (w *SSHWorker) IncrConn() {
	w.activeConns.Add(1)
}

func (w *SSHWorker) DecrConn() {
	w.activeConns.Add(-1)
}

func (w *SSHWorker) Latency() time.Duration {
	return time.Duration(w.latencyMs.Load()) * time.Millisecond
}

func (w *SSHWorker) SetLatency(d time.Duration) {
	w.latencyMs.Store(d.Milliseconds())
}

func (w *SSHWorker) setState(s State) {
	w.state.Store(int32(s))
	observability.Info("Worker state changed", "worker_id", w.cfg.ID, "state", s.String())
}

func (w *SSHWorker) Start(parentCtx context.Context) {
	ctx, cancel := context.WithCancel(parentCtx)
	w.mu.Lock()
	w.cancelLoop = cancel
	w.mu.Unlock()

	w.setState(StateStarting)
	w.wg.Add(1)
	go w.runLoop(ctx)
}

func (w *SSHWorker) Stop() {
	w.mu.Lock()
	if w.cancelLoop != nil {
		w.cancelLoop()
	}
	w.mu.Unlock()
	w.setState(StateStopping)
	w.wg.Wait()
	w.cleanup()
	w.setState(StateStopped)
}

func (w *SSHWorker) runLoop(ctx context.Context) {
	defer w.wg.Done()
	backoff := NewBackoff(1*time.Second, 5*time.Minute)
	attempt := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		w.setState(StateConnecting)
		err := w.connect(ctx)
		if err != nil {
			observability.Error("Worker connection failed", "worker_id", w.cfg.ID, "err", err)
			w.healthMgr.Report(w.cfg.ID, health.StatusUnhealthy, err)
			w.setState(StateReconnecting)

			delay := backoff.Duration(attempt)
			attempt++
			observability.Info("Worker backing off before reconnect", "worker_id", w.cfg.ID, "delay_sec", delay.Seconds())

			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			continue
		}

		attempt = 0
		w.setState(StateReady)
		w.healthMgr.Report(w.cfg.ID, health.StatusHealthy, nil)
		w.setState(StateRunning)

		err = w.monitor(ctx)
		if err != nil {
			observability.Warn("Worker connection lost", "worker_id", w.cfg.ID, "err", err)
			w.healthMgr.Report(w.cfg.ID, health.StatusDegraded, err)
			w.setState(StateDegraded)
		}

		w.cleanup()
	}
}

func (w *SSHWorker) connect(ctx context.Context) error {
	dialer := &net.Dialer{Timeout: w.cfg.ConnectTimeout}
	target := fmt.Sprintf("%s:%d", w.cfg.Host, w.cfg.Port)

	dialAddr := target
	if w.cfg.RemoteProxy != "" {
		dialAddr = w.cfg.RemoteProxy
	}

	conn, err := dialer.DialContext(ctx, "tcp", dialAddr)
	if err != nil {
		return err
	}

	// Jika lewat remote proxy, kirim HTTP CONNECT / payload dulu
	if w.cfg.RemoteProxy != "" && w.injector != nil {
		if err := w.injector.Inject(ctx, conn, target); err != nil {
			conn.Close()
			return fmt.Errorf("payload injection failed: %w", err)
		}
		observability.Debug("Payload injected through remote proxy",
			"worker_id", w.cfg.ID, "proxy", w.cfg.RemoteProxy, "target", target)
	}

	rawConnPtr := &conn
	w.conn.Store(rawConnPtr)

	var auths []ssh.AuthMethod
	if w.cfg.Password != "" {
		auths = append(auths, ssh.Password(w.cfg.Password))
	}

	sshConfig := &ssh.ClientConfig{
		Config: ssh.Config{
			Ciphers: []string{
				"aes128-gcm@openssh.com",
				"chacha20-poly1305@openssh.com",
				"aes128-ctr",
				"aes192-ctr",
				"aes256-ctr",
				"aes128-cbc",
				"3des-cbc",
			},
		},
		User:            w.cfg.Username,
		Auth:            auths,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		HostKeyAlgorithms: []string{
			ssh.KeyAlgoED25519,
			ssh.KeyAlgoRSA,
			ssh.KeyAlgoRSASHA256,
			ssh.KeyAlgoRSASHA512,
			ssh.KeyAlgoECDSA256,
			ssh.KeyAlgoECDSA384,
			ssh.KeyAlgoECDSA521,
		},
		Timeout: w.cfg.ConnectTimeout,
	}

	c, chans, reqs, err := ssh.NewClientConn(conn, target, sshConfig)
	if err != nil {
		conn.Close()
		return err
	}

	client := ssh.NewClient(c, chans, reqs)
	w.client.Store(client)
	w.setState(StateConnected)
	return nil
}

func (w *SSHWorker) monitor(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			client := w.client.Load()
			if client == nil {
				return fmt.Errorf("ssh client is nil")
			}
			_, _, err := client.SendRequest("keepalive@proxy-engine", true, nil)
			if err != nil {
				return err
			}
		}
	}
}

func (w *SSHWorker) cleanup() {
	if client := w.client.Load(); client != nil {
		client.Close()
		w.client.Store(nil)
	}
	if connPtr := w.conn.Load(); connPtr != nil {
		if c := *connPtr; c != nil {
			c.Close()
		}
		w.conn.Store(nil)
	}
}

func (w *SSHWorker) DialChannel(ctx context.Context, nt, addr string) (net.Conn, error) {
	client := w.client.Load()
	if client == nil {
		return nil, fmt.Errorf("worker not connected")
	}
	return client.Dial(nt, addr)
}