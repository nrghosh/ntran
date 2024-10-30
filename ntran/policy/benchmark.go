package policy

import (
	"log"
	"time"
)

type Benchmark struct {
	startTime time.Time
	endTime   time.Time
}

func (b *Benchmark) Start() {
	b.startTime = time.Now()
}

func (b *Benchmark) End() {
	b.endTime = time.Now()
}

func (b *Benchmark) Log(series int) {
	duration := b.endTime.Sub(b.startTime)
	logger := log.Default()
	logger.Printf("Series: %v | Duration: %v\n", series, duration)
}
