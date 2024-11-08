package policy

import (
	"log"
	"time"
)

type Benchmark struct {
	Policy    string
	TestCase  string
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
	logger.Printf("Policy: %v | Test Case: %v | Series: %v | Duration: %v\n", b.Policy, b.TestCase, series, duration)
}
