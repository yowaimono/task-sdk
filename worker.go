package tasksdk

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	schedulerv1 "github.com/yowaimono/task-sdk/api/gen"
	"github.com/yowaimono/task-sdk/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Config struct {
	SchedulerInstances []string
	WorkerID           string
	Roles              []string
	Slots              int
	DefaultTimeout     time.Duration
	WorkerToken        string
	TLSConfig          *tls.Config
}

type Builder struct {
	cfg    Config
	defs   map[string]task.Definition
	err    error
	port   string
	dialer func(context.Context, string) (net.Conn, error)
}

func Worker() *Builder { return &Builder{defs: make(map[string]task.Definition), port: "8090"} }
func (b *Builder) Config(cfg *Config) *Builder {
	if cfg == nil {
		b.err = errors.New("worker config must not be nil")
		return b
	}
	b.cfg = *cfg
	return b
}
func (b *Builder) SetServerPort(port string) *Builder { b.port = strings.TrimSpace(port); return b }
func DefineTask(name, role string, handler task.Task) task.Definition {
	return task.Definition{Name: name, Role: role, Handler: handler}
}
func (b *Builder) Register(def task.Definition) *Builder {
	if def.Name == "" || def.Role == "" || def.Handler == nil {
		b.err = errors.New("task name, role and handler are required")
		return b
	}
	if _, exists := b.defs[def.Name]; exists {
		b.err = fmt.Errorf("task %q already registered", def.Name)
		return b
	}
	b.defs[def.Name] = def
	return b
}

func (b *Builder) Start(ctx context.Context) error {
	if b.err != nil {
		return b.err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if b.cfg.WorkerID == "" {
		return errors.New("worker id is required")
	}
	if len(b.cfg.SchedulerInstances) == 0 {
		return errors.New("at least one scheduler instance is required")
	}
	if b.cfg.Slots < 1 {
		b.cfg.Slots = 1
	}
	if b.cfg.DefaultTimeout <= 0 {
		b.cfg.DefaultTimeout = 30 * time.Second
	}
	go b.serveHealth(ctx)
	return b.run(ctx)
}

func (b *Builder) serveHealth(ctx context.Context) {
	port, err := strconv.Atoi(strings.TrimPrefix(b.port, ":"))
	if err != nil || port <= 0 {
		return
	}
	srv := &http.Server{Addr: ":" + strconv.Itoa(port), Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	_ = srv.ListenAndServe()
}

func (b *Builder) run(ctx context.Context) error {
	var lastErr error
	for {
		for _, endpoint := range b.cfg.SchedulerInstances {
			if err := b.connectAndWork(ctx, endpoint); err == nil || errors.Is(err, context.Canceled) {
				return err
			} else {
				lastErr = err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
		if lastErr == nil {
			lastErr = errors.New("scheduler connection failed")
		}
	}
}

func (b *Builder) connectAndWork(ctx context.Context, endpoint string) error {
	transportCreds := credentials.TransportCredentials(insecure.NewCredentials())
	if b.cfg.TLSConfig != nil {
		transportCreds = credentials.NewTLS(b.cfg.TLSConfig)
	}
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(transportCreds)}
	if b.dialer != nil {
		dialOptions = append(dialOptions, grpc.WithContextDialer(b.dialer))
	}
	conn, err := grpc.DialContext(ctx, endpoint, dialOptions...)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := schedulerv1.NewSchedulerClient(conn)
	streamCtx := ctx
	if b.cfg.WorkerToken != "" {
		streamCtx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+b.cfg.WorkerToken)
	}
	if _, err = client.RegisterWorker(streamCtx, &schedulerv1.RegisterWorkerRequest{WorkerId: b.cfg.WorkerID, Roles: b.cfg.Roles, Slots: uint32(b.cfg.Slots)}); err != nil {
		return err
	}
	stream, err := client.Work(streamCtx)
	if err != nil {
		return err
	}
	var sendMu sync.Mutex
	send := func(m *schedulerv1.WorkerMessage) error { sendMu.Lock(); defer sendMu.Unlock(); return stream.Send(m) }
	if err := send(&schedulerv1.WorkerMessage{Kind: "hello", WorkerId: b.cfg.WorkerID}); err != nil {
		return err
	}
	pullStop := make(chan struct{})
	defer close(pullStop)
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = send(&schedulerv1.WorkerMessage{Kind: "pull", WorkerId: b.cfg.WorkerID, Pull: &schedulerv1.PullRequest{MaxTasks: 1}})
			case <-pullStop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	for i := 0; i < b.cfg.Slots; i++ {
		if err := send(&schedulerv1.WorkerMessage{Kind: "pull", WorkerId: b.cfg.WorkerID, Pull: &schedulerv1.PullRequest{MaxTasks: 1}}); err != nil {
			return err
		}
	}
	sem := make(chan struct{}, b.cfg.Slots)
	var wg sync.WaitGroup
	for {
		msg, recvErr := stream.Recv()
		if recvErr != nil {
			wg.Wait()
			return recvErr
		}
		if msg.Assignment == nil {
			continue
		}
		sem <- struct{}{}
		assignment := msg.Assignment
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			def, ok := b.defs[assignment.TaskName]
			if !ok {
				_ = send(&schedulerv1.WorkerMessage{Kind: "result", WorkerId: b.cfg.WorkerID, Result: &schedulerv1.TaskResult{TaskId: assignment.TaskId, Error: "task not registered"}})
				return
			}
			timeout := b.cfg.DefaultTimeout
			if assignment.TimeoutMs > 0 {
				timeout = time.Duration(assignment.TimeoutMs) * time.Millisecond
			}
			taskCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			params := make(map[string]json.RawMessage, len(assignment.Params))
			for key, value := range assignment.Params {
				params[key] = append(json.RawMessage(nil), value...)
			}
			tc := task.NewContext(taskCtx, task.ContextMeta{TaskID: assignment.TaskId, TaskName: assignment.TaskName, Role: assignment.Role, Attempt: int(assignment.Attempt), Params: params})
			result, taskErr := def.Handler.Handle(tc)
			response := &schedulerv1.TaskResult{TaskId: assignment.TaskId, Success: taskErr == nil}
			if taskErr != nil {
				response.Error = taskErr.Error()
			} else if result != nil {
				response.Output = result.Data
			}
			_ = send(&schedulerv1.WorkerMessage{Kind: "result", WorkerId: b.cfg.WorkerID, Result: response})
			_ = send(&schedulerv1.WorkerMessage{Kind: "pull", WorkerId: b.cfg.WorkerID, Pull: &schedulerv1.PullRequest{MaxTasks: 1}})
		}()
	}
}
