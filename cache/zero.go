package cache

import (
	"context"
	"time"
)

type zeroCache struct{}

func NewZero() Cache {
	return zeroCache{}
}

func (c zeroCache) Get(k interface{}) (interface{}, bool) {
	return nil, false
}

func (c zeroCache) Set(k interface{}, v interface{}, exp time.Time) {}

func (c zeroCache) Delete(k interface{}) {}

func (c zeroCache) Count() int {
	return 0
}

func (c zeroCache) Copy(ctx context.Context) map[interface{}]interface{} {
	return make(map[interface{}]interface{})
}

func (c zeroCache) Prune(ctx context.Context) int {
	return 0
}

func (c zeroCache) Flush() {}
