package cache

import (
	"context"
	"time"
)

type Pruner interface {
	Prune(context.Context)
}

type PrunerConfig struct {
	Interval time.Duration
	LogFunc  func(string, ...interface{})
}

type pruner struct {
	cache Cache
	cfg   PrunerConfig
}

func NewPruner(c Cache, cfg PrunerConfig) Pruner {
	return &pruner{
		cache: c,
		cfg:   cfg,
	}
}

func (p *pruner) Prune(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()

	logFunc := p.cfg.LogFunc
	if logFunc == nil {
		logFunc = func(s string, i ...interface{}) {}
	}

	logFunc("starting cache prune loop")
	defer logFunc("stopping cache prune loop")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n := p.cache.Prune(ctx)
			logFunc("pruned %v items from the cache", n)
		}
	}
}
