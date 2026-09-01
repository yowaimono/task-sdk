package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type Context interface {
	context.Context
	TaskID() string
	TaskName() string
	Role() string
	Attempt() int
	Params() Params
	Param(key string) (any, bool)
	DecodeParam(key string, dst any) error
}

type Params interface {
	Get(key string) (any, bool)
	Decode(key string, dst any) error
}

type rawParams struct{ values map[string]json.RawMessage }

func NewParams(values map[string]json.RawMessage) Params {
	copyValues := make(map[string]json.RawMessage, len(values))
	for k, v := range values {
		copyValues[k] = append(json.RawMessage(nil), v...)
	}
	return rawParams{values: copyValues}
}

func (p rawParams) Get(key string) (any, bool) {
	raw, ok := p.values[key]
	if !ok {
		return nil, false
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, false
	}
	return value, true
}

func (p rawParams) Decode(key string, dst any) error {
	if dst == nil {
		return errors.New("destination must not be nil")
	}
	raw, ok := p.values[key]
	if !ok {
		return fmt.Errorf("parameter %q not found", key)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode parameter %q: %w", key, err)
	}
	return nil
}

type taskContext struct {
	context.Context
	taskID   string
	taskName string
	role     string
	attempt  int
	params   Params
}

type ContextMeta struct {
	TaskID   string
	TaskName string
	Role     string
	Attempt  int
	Params   map[string]json.RawMessage
}

func NewContext(parent context.Context, meta ContextMeta) Context {
	if parent == nil {
		parent = context.Background()
	}
	return &taskContext{Context: parent, taskID: meta.TaskID, taskName: meta.TaskName, role: meta.Role, attempt: meta.Attempt, params: NewParams(meta.Params)}
}

func (c *taskContext) TaskID() string                        { return c.taskID }
func (c *taskContext) TaskName() string                      { return c.taskName }
func (c *taskContext) Role() string                          { return c.role }
func (c *taskContext) Attempt() int                          { return c.attempt }
func (c *taskContext) Params() Params                        { return c.params }
func (c *taskContext) Param(key string) (any, bool)          { return c.params.Get(key) }
func (c *taskContext) DecodeParam(key string, dst any) error { return c.params.Decode(key, dst) }
