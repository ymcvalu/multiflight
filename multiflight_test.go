package multiflight

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMultiflight(t *testing.T) {
	const (
		AllKeysNum = 500  // num of all keys
		WorkerNum  = 1000 // num of goroutines
		LoopTimes  = 5    // load time every goroutine
		BatchSize  = 20   // num of keys every batch
	)

	var stats = struct {
		total            uint32
		timesByBatchSize [BatchSize + 1]uint32
	}{}
	loader := func(ctx context.Context, keys []int) (map[int]string, error) {
		sleep := rand.Int63n(20)
		time.Sleep(time.Millisecond * time.Duration(sleep))

		atomic.AddUint32(&stats.total, 1)
		atomic.AddUint32(&stats.timesByBatchSize[len(keys)], 1)
		resuts := make(map[int]string, len(keys))
		for _, k := range keys {
			resuts[k] = fmt.Sprintf("val: %d", k)
		}

		return resuts, nil
	}

	keys := make([]int, 0, AllKeysNum)
	for i := 0; i < AllKeysNum; i++ {
		keys = append(keys, i)
	}

	wg := sync.WaitGroup{}
	g := Group[int, string]{}
	ast := assert.New(t)
	for i := 0; i < WorkerNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < LoopTimes; i++ {
				from := rand.Intn(AllKeysNum - BatchSize)
				to := from + BatchSize
				loadKeys := keys[from:to]
				results, err := g.Do(context.Background(), loadKeys, loader)
				ast.Nil(err)
				ast.Len(results, len(loadKeys))
				for _, key := range loadKeys {
					v, has := results[key]
					ast.True(has)
					ast.Equal(fmt.Sprintf("val: %d", key), v)
				}
			}
		}()
	}

	wg.Wait()
	ast.Equal(0, len(g.m))
	t.Logf("load times: %d", stats.total)
	t.Log(stats.timesByBatchSize)
}
