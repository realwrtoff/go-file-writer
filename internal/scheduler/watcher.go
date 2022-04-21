package scheduler

import (
	"context"
	"github.com/go-redis/redis"
	"time"
)

type Watcher struct {
	rds *redis.Client
}

func NewWatcher(rds *redis.Client) *Watcher {
	return &Watcher{
		rds: rds,
	}
}

func (w *Watcher) Run(ctx context.Context) {
	ticker := time.Tick(time.Minute)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker:
			// redis was always disconneted although related options were set.
			w.rds.Ping()
		}
	}
}
