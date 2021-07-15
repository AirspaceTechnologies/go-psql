package cache

import (
	"context"
	"sync"
	"time"
)

type rwCache struct {
	mut sync.RWMutex

	m map[interface{}]item
}

func NewRW() Cache {
	return &rwCache{
		m: make(map[interface{}]item),
	}
}

func (c *rwCache) Get(k interface{}) (interface{}, bool) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	i, ok := c.m[k]
	if !ok || i.Expired() {
		return nil, false
	}

	return i.Value, true
}

func (c *rwCache) Set(k interface{}, v interface{}, exp time.Time) {
	c.mut.Lock()
	defer c.mut.Unlock()

	var expStamp int64
	if !exp.IsZero() {
		expStamp = exp.UnixNano()
	}

	c.m[k] = item{
		Value:   v,
		Expires: expStamp,
	}
}

func (c *rwCache) Delete(k interface{}) {
	c.mut.Lock()
	defer c.mut.Unlock()

	c.delete(k, nil)
}

func (c *rwCache) Count() int {
	c.mut.RLock()
	defer c.mut.RUnlock()

	return len(c.m)
}

func (c *rwCache) Copy(ctx context.Context) map[interface{}]interface{} {
	c.mut.RLock()
	defer c.mut.RUnlock()

	m := make(map[interface{}]interface{}, len(c.m))
	for k, v := range c.m {
		if ctx.Err() != nil {
			return m
		}

		m[k] = v.Value
	}

	return m
}

func (c *rwCache) Prune(ctx context.Context) int {
	keys := func() []interface{} {
		c.mut.RLock()
		defer c.mut.RUnlock()

		keys := make([]interface{}, 0, len(c.m))
		for k, v := range c.m {
			if ctx.Err() != nil {
				return nil
			}

			if v.Expired() {
				keys = append(keys, k)
			}
		}
		return keys
	}()

	if ctx.Err() != nil {
		return 0
	}

	var n int
	func() {
		c.mut.Lock()
		defer c.mut.Unlock()

		for _, k := range keys {
			if ctx.Err() != nil {
				return
			}

			c.delete(k, func(i item) bool {
				return i.Expired()
			})
		}
	}()

	return n
}

func (c *rwCache) Flush() {
	c.mut.Lock()
	defer c.mut.Unlock()

	c.m = make(map[interface{}]item)
}

////////////////////////////////////////////////////////////////////////////////

func (c *rwCache) delete(k interface{}, deleteIf func(item) bool) {
	if deleteIf == nil {
		delete(c.m, k)
		return
	}

	i, ok := c.m[k]
	if !ok {
		return
	}

	if deleteIf(i) {
		delete(c.m, k)
	}
}
