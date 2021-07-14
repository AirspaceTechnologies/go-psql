package cache

import (
	"context"
	"time"
)

type Cache interface {
	Get(interface{}) (interface{}, bool)
	Set(interface{}, interface{}, time.Time)
	Delete(interface{})
	Count() int
	Copy(context.Context) map[interface{}]interface{}
	Prune(context.Context) int
	Flush()
}
