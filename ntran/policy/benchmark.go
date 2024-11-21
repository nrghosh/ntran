package policy

import (
	"fmt"
	"log"
	"time"
)

type Benchmark struct {
	Experiment       *Experiment
	Policy           string
	TestCase         string
	TransactionCount int
	startTime        time.Time
	endTime          time.Time
}

func (b *Benchmark) Start() {
	b.startTime = time.Now()
}

func (b *Benchmark) End() {
	b.endTime = time.Now()
}

func (b *Benchmark) Log() {
	duration := b.endTime.Sub(b.startTime)
	logger := log.Default()
	logger.Printf("Policy: %v | Test Case: %v | Transaction Count: %v | Duration: %v\n", b.Policy, b.TestCase, b.TransactionCount, duration)
	b.Experiment.Log(Record{
		Policy:           b.Policy,
		TestCase:         b.TestCase,
		TransactionCount: fmt.Sprintf("%d", b.TransactionCount),
		Duration:         duration.String(),
	})
}
