package worker

import (
	"context"
	"fmt"
	"net"
	"time"

	"core-proxy/pkg/control/payload"
)

type PayloadDialer struct {
	rawDialer net.Dialer
	injector  *payload.Injector
}

func NewPayloadDialer(rawPattern string, timeout time.Duration) (*PayloadDialer, error) {
	inj, err := payload.NewInjector(rawPattern, 10*time.Millisecond)
	if err != nil {
		return nil, err
	}

	return &PayloadDialer{
		rawDialer: net.Dialer{Timeout: timeout},
		injector:  inj,
	}, nil
}

func (pd *PayloadDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := pd.rawDialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	if pd.injector != nil {
		if err := pd.injector.Inject(ctx, conn, address); err != nil {
			conn.Close()
			return nil, fmt.Errorf("payload injection failed: %w", err)
		}
	}

	return conn, nil
}

