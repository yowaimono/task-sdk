# task-sdk

Go SDK for registering and running workers against the task scheduler.

## Install

```bash
go get github.com/yowaimono/task-sdk
```

## Example

```go
package main

import (
    "context"
    "log"

    tasksdk "github.com/yowaimono/task-sdk"
    "github.com/yowaimono/task-sdk/task"
)

func main() {
    worker := tasksdk.Worker().
        Config(&tasksdk.Config{
            WorkerID: "image-worker-01",
            Roles: []string{"image"},
            Slots: 4,
            SchedulerInstances: []string{"scheduler.example.com:9090"},
            WorkerToken: "replace-me",
        }).
        Register(tasksdk.DefineTask("image.resize", "image", task.Func(func(ctx task.Context) (*task.Result, error) {
            var input struct { Source string `json:"source"` }
            if err := ctx.DecodeParam("input", &input); err != nil { return nil, err }
            return &task.Result{Data: []byte(input.Source)}, nil
        })))

    if err := worker.Start(context.Background()); err != nil { log.Fatal(err) }
}
```

`task.Context` is a real `context.Context` and adds task metadata plus typed parameter decoding. Cancellation and deadlines propagate through `Done`, `Err`, and `Deadline`.

## Protocol

The `api/gen` directory contains generated protobuf/gRPC bindings for the scheduler worker protocol. It is included so consumers only need this module.

## Development

```bash
go test ./...
go vet ./...
```

The SDK uses a persistent bidirectional gRPC stream, reconnects across scheduler instances, sends worker token metadata when configured, and keeps local execution bounded by `Slots` and task deadlines.
