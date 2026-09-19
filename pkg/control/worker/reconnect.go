package worker

import (
	"math/rand"
	"time"
)

type Backoff struct {
	Base   time.Duration
	Max    time.Duration
	factor float64
}

func NewBackoff(base, max time.Duration) *Backoff {
	return &Backoff{
		Base:   base,
		Max:    max,
		factor: 2.0,
	}
}

func (b *Backoff) Duration(attempt int) time.Duration {
	temp := float64(b.Base)
	for i := 0; i < attempt; i++ {
		temp *= b.factor
		if temp >= float64(b.Max) {
			temp = float64(b.Max)
			break
		}
	}
	sleep := time.Duration(rand.Float64() * temp)
	if sleep < b.Base {
		sleep = b.Base
	}
	if sleep > b.Max {
		sleep = b.Max
	}
	return sleep
}

