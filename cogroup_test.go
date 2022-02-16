package cogroup

import (
	"context"
	"testing"
	"time"
)

func TestCogroup(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
	g := Start(ctx, 4, 20, false)
	br := true
	for ; br; {
		f := func(ctx1 context.Context) error {
			//t.Log(GetWorkerID(ctx1), "xxxxxxxxxxxxxxxxxx")
			return nil
		}
		select {
		case <-ctx.Done():
			t.Log("跳出")
			br = false
		default:
			t.Log("写入")
			g.Insert(f)
		}
	}
	t.Log("the end wait")
	num := g.Wait()
	t.Log("the end, remain ", num)
}
