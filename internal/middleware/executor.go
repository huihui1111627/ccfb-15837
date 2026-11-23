package middleware

import (
	"context"
	"sync"
)

// Executor 受管控的线程池:提交任务时捕获上下文,
// 任务在线程中执行时自动恢复压测标记,业务代码无需手动传递。
type Executor struct {
	service string
	tasks   chan execTask
	wg      sync.WaitGroup
}

type execTask struct {
	stress bool
	trace  string
	uid    int64
	fn     func(context.Context)
}

func NewExecutor(service string, workers, queue int) *Executor {
	e := &Executor{service: service, tasks: make(chan execTask, queue)}
	for i := 0; i < workers; i++ {
		go e.loop()
	}
	return e
}

func (e *Executor) loop() {
	for t := range e.tasks {
		ctx := context.Background()
		if t.stress {
			ctx = WithStress(ctx, true)
			M.RecordPropagation(e.service, "threadpool", true)
		}
		ctx = WithTrace(ctx, t.trace)
		ctx = WithUser(ctx, t.uid)
		t.fn(ctx)
		e.wg.Done()
	}
}

// Submit 提交异步任务,上下文随任务一起透传到工作线程。
func (e *Executor) Submit(ctx context.Context, fn func(context.Context)) {
	e.wg.Add(1)
	e.tasks <- execTask{stress: IsStress(ctx), trace: TraceID(ctx), uid: UserID(ctx), fn: fn}
}

func (e *Executor) Wait() { e.wg.Wait() }

func (e *Executor) Close() {
	e.wg.Wait()
	close(e.tasks)
}
