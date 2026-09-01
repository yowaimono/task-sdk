package tasksdk

import (
	"context"
	"testing"

	"github.com/yowaimono/task-sdk/task"
)

type noopTask struct{}

func (noopTask) Handle(task.Context) (*task.Result, error) { return &task.Result{}, nil }

func TestBuilderValidationAndDuplicateRegistration(t *testing.T) {
	b := Worker().Config(&Config{WorkerID: "w", SchedulerInstances: []string{"localhost:9090"}})
	b.Register(DefineTask("demo", "default", noopTask{})).Register(DefineTask("demo", "default", noopTask{}))
	if err := b.Start(context.Background()); err == nil {
		t.Fatal("expected duplicate registration error")
	}
}

func TestBuilderRequiresWorkerID(t *testing.T) {
	if err := Worker().Config(&Config{SchedulerInstances: []string{"localhost:9090"}}).Start(context.Background()); err == nil {
		t.Fatal("expected worker id validation error")
	}
}

func TestBuilderValidationEdges(t *testing.T) {
	if err := Worker().Config(nil).Start(context.Background()); err == nil {
		t.Fatal("nil config accepted")
	}
	if err := Worker().Config(&Config{WorkerID: "w"}).Start(context.Background()); err == nil {
		t.Fatal("missing scheduler list accepted")
	}
	if err := Worker().Config(&Config{WorkerID: "w", SchedulerInstances: []string{"x"}}).Register(DefineTask("", "role", noopTask{})).Start(context.Background()); err == nil {
		t.Fatal("invalid task definition accepted")
	}
}
