package cogroup

import (
	"context"
	"fmt"
	"github.com/RussellLuo/timingwheel"
	"github.com/google/uuid"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

var TimeWheel = timingwheel.NewTimingWheel(time.Millisecond*100, 110) // max 10s min 100ms

type EveryScheduler struct {
	Interval time.Duration
}

func (s *EveryScheduler) Next(prev time.Time) time.Time {
	return prev.Add(s.Interval)
}

type Worker func(ctx context.Context, i int, f func(context.Context) error)

var scheduler = &EveryScheduler{Interval: time.Second}

type CoGroup struct {
	id     string                           // 用于Bug追踪，排查，无功能性作用
	worker Worker                           // 方法绑定
	ctx    context.Context                  // Group context,NOTE:不要滥用,单平面信号只会过一次
	wg     sync.WaitGroup                   // Group goroutine wait group
	ch     chan func(context.Context) error // Task chan
	n      int                              // 启用协程数
	stop   bool                             // 是否已经停止
	smooth bool                             // 是否平滑过渡，执行完所有消息
	m      sync.RWMutex                     //
	exit   chan struct{}                    // 退出信号，等量n，除了process()调用，其他借调者一律归还
}

// Worker meta context key
type workerKey struct{}

// New will create a cogroup instance without starting the group
func new(ctx context.Context, n uint, m uint, smooth bool) *CoGroup {
	if n < 1 {
		panic("At least one goroutine should spawned in cogroup!")
	}
	return &CoGroup{
		ctx:    ctx,
		id:     uuid.NewString(),
		ch:     make(chan func(context.Context) error, m),
		n:      int(n),
		smooth: smooth,
		exit:   make(chan struct{}, 1),
	}
}

// 监听退出信号，广播到所有的process中
func (g *CoGroup) signMonitor() {
	select {
	case <-g.ctx.Done():
		g.exit <- struct{}{}
	}
}

func Start(ctx context.Context, n uint, m uint, smooth bool) *CoGroup {
	g := new(ctx, n, m, smooth)
	g.worker = g.run
	g.start(g.n)
	return g
}

// Insert a task into the task queue with blocking if the task queue buffer is full.
// If the group context was canceled already, it will abort with a false return.
func (g *CoGroup) Insert(f func(context.Context) error) (success bool) {
	if g.IsStop() {
		return false
	}
	defer func() {
		if r := recover(); r != nil {
			success = false
		}
	}()
	for {
		select {
		case g.ch <- f:
			return true
		case sign := <-g.exit: // 方式isStop状态在大并发下放进来后已经变更
			g.exit <- sign
			return
		}
	}
}
func (g *CoGroup) IsStop() bool {
	g.m.RLock()
	defer g.m.RUnlock()
	return g.stop
}

// Start the coroutine group
func (g *CoGroup) start(n int) {
	go g.signMonitor()
	for i := 0; i < n; i++ {
		g.wg.Add(1)
		go g.process(i)
	}
}

// finally 锁定并发process 0 做最后的回收，简单防并发抢占
func (g *CoGroup) finally(i int) {
	if i > 0 {
		return
	}
	g.m.Lock()
	defer g.m.Unlock()
	if g.stop {
		return
	}
	g.stop = true
	if !g.smooth {
		close(g.ch)
		return
	}
	finishSign := make(chan struct{}, 1)
	tw := TimeWheel.ScheduleFunc(scheduler, func() {
		if g.Size() == 0 {
			close(g.ch)
			finishSign <- struct{}{}

		}
	})
	defer func() {
		for !tw.Stop() {
		}
	}()
	go func() {
		for ch := range g.ch {
			g.worker(g.ctx, i, ch)
		}
	}()
	select {
	case <-finishSign:
		break
	}
}

// Start a single coroutine
func (g *CoGroup) process(i int) {
	defer g.processDone()
	for {
		select {
		case <-g.exit:
			g.finally(i)
			return
		case f := <-g.ch:
			g.worker(g.ctx, i, f)
		}
	}
}

func (g *CoGroup) processDone() {
	g.exit <- struct{}{}
	g.wg.Done()
}

// Execute a single task
func (g *CoGroup) run(_ context.Context, i int, f func(context.Context) error) {
	if f == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "CoGroup panic captured: %s", debug.Stack())
		}
	}()
	if g.smooth {
		_ = f(context.WithValue(context.Background(), workerKey{}, i))
	} else {
		_ = f(context.WithValue(g.ctx, workerKey{}, i))
	}
	return
}

// Size return the current length the task queue
func (g *CoGroup) Size() int {
	return len(g.ch)
}

// Wait till the tasks in queue are all finished, or the group was canceled by the context.
func (g *CoGroup) Wait() int {
	g.wg.Wait()
	close(g.exit)
	return len(g.ch)
}

// GetWorkers Get the number of total group workers
func (g *CoGroup) GetWorkers() int {
	return g.n
}

// GetWorkerID Get worker id from the context
func GetWorkerID(ctx context.Context) int {
	n, _ := ctx.Value(workerKey{}).(int)
	return n
}
