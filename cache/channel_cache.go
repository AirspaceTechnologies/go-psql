package cache

import (
	"sync"
	"time"
)

type channelCache struct {
	mut sync.Mutex

	Cache
	channels map[interface{}]chan <-chanFunc
	buffSize int
}

func NewChannelCache(c Cache, buffSize int) QueuedCache {
	if buffSize < 1 {
		buffSize = 1
	}

	return &channelCache{
		Cache:    c,
		channels: make(map[interface{}]chan <-chanFunc),
		buffSize: buffSize,
	}
}

func (cc *channelCache) QueuedGetOrSet(k interface{}, f func() (interface{}, error), exp time.Time) (interface{}, error) {
	if v, ok := cc.Get(k); ok {
		return v, nil
	}

	r := <-cc.queueUp(k, f, exp)
	return r.V, r.Err
}

func (cc *channelCache) queueUp(k interface{}, f func() (interface{}, error), exp time.Time) <-chan chanFuncResponse {
	cc.mut.Lock()
	defer cc.mut.Unlock()

	rc := make(chan chanFuncResponse, 1)

	cf := chanFunc{
		C: rc,
		F: f,
	}
	ch, ok := cc.channels[k]
	if ok {
		select {
		case ch <- cf:
		default:
			go func() {
				cfr := chanFuncResponse{}
				cfr.V, cfr.Err = f()
				rc <- cfr
			}()
		}
	} else {
		ch := make(chan chanFunc, cc.buffSize)
		ch <- cf
		cc.channels[k] = ch
		go func() {
			var cfr *chanFuncResponse
			for {
				select {
				case chFunc := <-ch:
					cfr = cc.response(k, chFunc, cfr, exp)
				default:
					cc.deleteChan(k)
					for chFunc := range ch {
						cfr = cc.response(k, chFunc, cfr, exp)
					}
				}
			}
		}()
	}

	return rc
}

func (cc *channelCache) deleteChan(k interface{}) {
	cc.mut.Lock()
	defer cc.mut.Unlock()

	delete(cc.channels, k)
}

func (cc *channelCache) response(k interface{}, cf chanFunc, cfr *chanFuncResponse, exp time.Time) *chanFuncResponse {
	if cfr != nil {
		cf.C <- *cfr
		return cfr
	}

	cfr = &chanFuncResponse{}
	cfr.V, cfr.Err = cf.F()
	if cfr.Err == nil {
		cc.Set(k, cfr.V, exp)
	}
	cf.C <- *cfr

	return cfr
}

type chanFunc struct {
	C chan<- chanFuncResponse
	F func() (interface{}, error)
}

type chanFuncResponse struct {
	V interface{}
	Err error
}
