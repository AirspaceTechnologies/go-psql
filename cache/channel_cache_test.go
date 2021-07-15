package cache

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChannelCache_QueuedGetOrSet(t *testing.T) {
	for i, baseCache := range []Cache{NewRW(), NewSync()} {
		t.Run(fmt.Sprintf("test %v", i), func(t *testing.T) {
			buffSize := 5
			cache := NewChannelCache(baseCache, buffSize)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go NewPruner(cache, PrunerConfig{
				Interval: time.Millisecond*10,
			}).Prune(ctx)

			k := "some_key"
			var wg sync.WaitGroup
			var currentVal int64
			var funcRuns uint64
			n := 200
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func(i int64) {
					defer wg.Done()

					var swapped bool
					var ran bool
					v, err := cache.QueuedGetOrSet(k, func() (interface{}, error) {
						time.Sleep(100*time.Millisecond)
						atomic.AddUint64(&funcRuns, 1)
						swapped = atomic.CompareAndSwapInt64(&currentVal, 0, i)
						ran = true
						return i, nil
					}, time.Now().Add(time.Second))
					if ran {
						require.Equal(t, i, v)
					} else if swapped {
						require.Equal(t, atomic.LoadInt64(&currentVal), v)
					}

					require.Nil(t, err)
				}(int64(i+1))
			}
			wg.Wait()

			require.Equal(t, n - buffSize, int(funcRuns))
		})
	}
}
