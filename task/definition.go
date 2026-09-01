package task

import "time"

type Result struct {
	Data []byte `json:"data,omitempty"`
}

type Task interface {
	Handle(Context) (*Result, error)
}

type Func func(Context) (*Result, error)

func (f Func) Handle(ctx Context) (*Result, error) { return f(ctx) }

type Definition struct {
	Name    string
	Role    string
	Timeout time.Duration
	Handler Task
}
