package task

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestContextParametersAndCancellation(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx := NewContext(parent, ContextMeta{TaskID: "t-1", TaskName: "demo", Role: "default", Attempt: 2, Params: map[string]json.RawMessage{"count": json.RawMessage(`3`)}})
	if ctx.TaskID() != "t-1" || ctx.Attempt() != 2 {
		t.Fatalf("metadata not preserved")
	}
	var count int
	if err := ctx.DecodeParam("count", &count); err != nil || count != 3 {
		t.Fatalf("decode = %d, %v", count, err)
	}
	if _, ok := ctx.Param("missing"); ok {
		t.Fatal("missing parameter reported as present")
	}
	cancel()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("err = %v", ctx.Err())
	}
}
