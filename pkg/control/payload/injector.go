package payload

import (
	"context"
	"fmt"
	"net"
	"time"

	"core-proxy/pkg/common/observability"
)

type Injector struct {
	template *Template
	delay    time.Duration
}

func NewInjector(rawPattern string, splitDelay time.Duration) (*Injector, error) {
	tmpl, err := ParseTemplate(rawPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to parse payload template: %w", err)
	}

	return &Injector{
		template: tmpl,
		delay:    splitDelay,
	}, nil
}

func (inj *Injector) Inject(ctx context.Context, conn net.Conn, targetAddr string) error {
	info, err := ExtractTargetInfo(targetAddr)
	if err != nil {
		return err
	}

	segments, err := inj.template.BuildSegments(info)
	if err != nil {
		return fmt.Errorf("failed to build payload segments: %w", err)
	}

	for idx, seg := range segments {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if idx > 0 && inj.delay > 0 {
			time.Sleep(inj.delay)
		}

		_, err := conn.Write(seg)
		if err != nil {
			return fmt.Errorf("failed writing payload segment %d/%d: %w", idx+1, len(segments), err)
		}

		observability.Debug("Injected payload segment", "segment_index", idx, "bytes", len(seg))
	}

	return nil
}

