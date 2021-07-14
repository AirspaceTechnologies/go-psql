package cache

import (
	"context"
	"sync"
	"time"
)

type syncCache struct {
	m sync.Map
}

func NewSync() Cache {
	return &syncCache{}
}

func (c *syncCache) Get(k interface{}) (interface{}, bool) {
	el, ok := c.m.Load(k)
	if !ok {
		return nil, false
	}

	i := el.(item)
	if i.Expired() {
		return nil, false
	}

	return i.Value, true
}

func (c *syncCache) Set(k interface{}, v interface{}, exp time.Time) {
	var expStamp int64
	if !exp.IsZero() {
		expStamp = exp.UnixNano()
	}

	c.m.Store(k, item{
		Value:   v,
		Expires: expStamp,
	})
}

func (c *syncCache) Delete(k interface{}) {
	c.m.Delete(k)
}

func (c *syncCache) Count() int {
	var n int
	c.m.Range(func(k, v interface{}) bool {
		n += 1
		return true
	})
	return n
}

func (c *syncCache) Copy(ctx context.Context) map[interface{}]interface{} {
	m := make(map[interface{}]interface{})
	c.m.Range(func(k, v interface{}) bool {
		if ctx.Err() != nil {
			return false
		}

		m[k] = v
		return true
	})
	return m
}

func (c *syncCache) Prune(ctx context.Context) int {
	var n int
	c.m.Range(func(k, v interface{}) bool {
		if ctx.Err() != nil {
			return false
		}

		if v.(item).Expired() {
			c.Delete(k)
			n += 1
		}

		return true
	})
	return n
}

func (c *syncCache) Flush() {
	c.m = sync.Map{}
}
