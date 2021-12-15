package scanner

import (
	"context"
	"time"
)

// ScheduledTask defines an interface where developers can extend to implements a scheduled task
type ScheduledTask interface {
	// Name returns the name of a task. The name should be unique
	Name() string

	// Enabled returns whether the task is enabled
	Enabled() bool

	// Interval returns the interval of the task execution
	Interval() time.Duration

	// Init setup the initial state of the task
	Init(ctx context.Context) error

	// Execute executes a task
	Execute(ctx context.Context) error
}
