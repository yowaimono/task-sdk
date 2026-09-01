package tasksdk

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	schedulerv1 "github.com/yowaimono/task-sdk/api/gen"
	"github.com/yowaimono/task-sdk/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type sdkTestServer struct {
	schedulerv1.UnimplementedSchedulerServer
	assignment      *schedulerv1.TaskAssignment
	result          chan *schedulerv1.TaskResult
	token           string
	pulls           int
	ignoreFirstPull bool
}

func (s *sdkTestServer) RegisterWorker(ctx context.Context, req *schedulerv1.RegisterWorkerRequest) (*schedulerv1.RegisterWorkerResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if values := md.Get("authorization"); len(values) > 0 {
		s.token = values[0]
	}
	if req.WorkerId == "" {
		return nil, errors.New("missing worker")
	}
	return &schedulerv1.RegisterWorkerResponse{SchedulerId: "test"}, nil
}
func (s *sdkTestServer) Work(stream schedulerv1.Scheduler_WorkServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		if msg.Kind == "pull" {
			s.pulls++
			if s.ignoreFirstPull && s.pulls == 1 {
				continue
			}
			if err := stream.Send(&schedulerv1.SchedulerMessage{Kind: "assignment", Assignment: s.assignment}); err != nil {
				return err
			}
		}
		if msg.Result != nil {
			s.result <- msg.Result
			return nil
		}
	}
}

func runSDKTestServer(t *testing.T, assignment *schedulerv1.TaskAssignment) (*Builder, *sdkTestServer, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	service := &sdkTestServer{assignment: assignment, result: make(chan *schedulerv1.TaskResult, 1)}
	server := grpc.NewServer()
	schedulerv1.RegisterSchedulerServer(server, service)
	go server.Serve(listener)
	builder := Worker()
	builder.dialer = func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	return builder, service, func() { server.Stop(); listener.Close() }
}

func TestWorkerSDKExecutesTaskAndPropagatesContext(t *testing.T) {
	builder, service, cleanup := runSDKTestServer(t, &schedulerv1.TaskAssignment{TaskId: "task-1", TaskName: "echo", Role: "default", Attempt: 2, Params: map[string][]byte{"message": []byte(`"hello"`)}})
	defer cleanup()
	handler := task.Func(func(ctx task.Context) (*task.Result, error) {
		var message string
		if err := ctx.DecodeParam("message", &message); err != nil {
			return nil, err
		}
		if message != "hello" || ctx.TaskID() != "task-1" || ctx.Attempt() != 2 {
			t.Fatalf("context mismatch message=%q id=%q attempt=%d", message, ctx.TaskID(), ctx.Attempt())
		}
		return &task.Result{Data: []byte("ok")}, nil
	})
	builder.Config(&Config{WorkerID: "worker-1", Roles: []string{"default"}, Slots: 1, SchedulerInstances: []string{"bufnet"}, WorkerToken: "secret"}).Register(DefineTask("echo", "default", handler))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- builder.connectAndWork(ctx, "bufnet") }()
	select {
	case result := <-service.result:
		if !result.Success || string(result.Output) != "ok" {
			t.Fatalf("result=%#v", result)
		}
		if service.token != "Bearer secret" {
			t.Fatalf("token=%q", service.token)
		}
	case <-ctx.Done():
		t.Fatal("worker did not return result")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestWorkerSDKReportsUnregisteredTask(t *testing.T) {
	builder, service, cleanup := runSDKTestServer(t, &schedulerv1.TaskAssignment{TaskId: "missing", TaskName: "unknown", Role: "default"})
	defer cleanup()
	builder.Config(&Config{WorkerID: "worker", Roles: []string{"default"}, Slots: 1, SchedulerInstances: []string{"bufnet"}})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go builder.connectAndWork(ctx, "bufnet")
	select {
	case result := <-service.result:
		if result.Success || !strings.Contains(result.Error, "not registered") {
			t.Fatalf("result=%#v", result)
		}
	case <-ctx.Done():
		t.Fatal("missing task result not reported")
	}
}

func TestWorkerSDKEnforcesAssignmentTimeout(t *testing.T) {
	builder, service, cleanup := runSDKTestServer(t, &schedulerv1.TaskAssignment{TaskId: "timeout", TaskName: "slow", Role: "default", TimeoutMs: 30})
	defer cleanup()
	handler := task.Func(func(ctx task.Context) (*task.Result, error) { <-ctx.Done(); return nil, ctx.Err() })
	builder.Config(&Config{WorkerID: "worker", Roles: []string{"default"}, Slots: 1, SchedulerInstances: []string{"bufnet"}, DefaultTimeout: time.Second}).Register(DefineTask("slow", "default", handler))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go builder.connectAndWork(ctx, "bufnet")
	select {
	case result := <-service.result:
		if result.Success || !strings.Contains(result.Error, "deadline exceeded") {
			t.Fatalf("result=%#v", result)
		}
	case <-ctx.Done():
		t.Fatal("timeout result not reported")
	}
}

func TestWorkerSDKPollsAfterInitiallyEmptyQueue(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	service := &sdkTestServer{assignment: &schedulerv1.TaskAssignment{TaskId: "idle-task", TaskName: "echo", Role: "default"}, result: make(chan *schedulerv1.TaskResult, 1), ignoreFirstPull: true}
	server := grpc.NewServer()
	schedulerv1.RegisterSchedulerServer(server, service)
	go server.Serve(listener)
	defer server.Stop()
	builder := Worker()
	builder.dialer = func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	builder.Config(&Config{WorkerID: "idle-worker", Roles: []string{"default"}, Slots: 1, SchedulerInstances: []string{"bufnet"}}).Register(DefineTask("echo", "default", task.Func(func(task.Context) (*task.Result, error) { return &task.Result{Data: []byte("polled")}, nil })))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- builder.connectAndWork(ctx, "bufnet") }()
	select {
	case result := <-service.result:
		if !result.Success || string(result.Output) != "polled" {
			t.Fatalf("result=%#v", result)
		}
		if service.pulls < 2 {
			t.Fatalf("pulls=%d, expected idle polling", service.pulls)
		}
		cancel()
	case <-ctx.Done():
		t.Fatal("worker did not poll after idle queue")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after context cancel")
	}
}

var _ = io.EOF
