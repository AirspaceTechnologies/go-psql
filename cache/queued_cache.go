package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

type QueuedCache interface {
	QueuedGetOrSet(interface{}, func() (interface{}, error), time.Time) (interface{}, error)
	Cache
}

type queuedCache struct {
	mut sync.Mutex

	Cache
	muts *mutexes
}

func NewQueuedCache(c Cache) QueuedCache {
	return &queuedCache{
		Cache: c,
		muts:  &mutexes{
			muts: make(map[interface{}]*mutex),
		},
	}
}

func (qc *queuedCache) QueuedGetOrSet(k interface{}, f func() (interface{}, error), exp time.Time) (interface{}, error) {
	if v, ok := qc.Get(k); ok {
		return v, nil
	}

	unlock := qc.muts.Lock(k)
	defer unlock()

	if v, ok := qc.Get(k); ok {
		return v, nil
	}

	v, err := f()
	if err == nil {
		qc.Set(k, v, exp)
	}

	return v, err
}

////////////////////////////////////////////////////////////////////////////////

type mutexes struct {
	mut sync.Mutex

	muts map[interface{}]*mutex
}

func (ms *mutexes) Lock(k interface{}) func() {
	mut := ms.mutForKey(k)
	atomic.AddUint64(&mut.count, 1)
	mut.Lock()
	return func() {
		atomic.AddUint64(&mut.count, ^uint64(0))
		done := atomic.LoadUint64(&mut.count) == 0
		mut.Unlock()

		if done {
			go func() {
				ms.mut.Lock()
				defer ms.mut.Unlock()

				if mut == ms.muts[k] && atomic.LoadUint64(&mut.count) == 0 {
					delete(ms.muts, k)
				}
			}()
		}
	}
}

func (ms *mutexes) mutForKey(k interface{}) *mutex {
	ms.mut.Lock()
	defer ms.mut.Unlock()

	if mut, ok := ms.muts[k]; ok {
		return mut
	}

	mut := &mutex{}
	ms.muts[k] = mut

	return mut
}

type mutex struct {
	sync.Mutex
	count uint64
}
