package reader

import (
	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
)

type ReaderInterface interface {
	ReadLine() string
}

type RdsReader struct {
	queue string
	rds *redis.Client
	runLog *logrus.Logger
}

func NewRdsReader(
	queue string,
	rds *redis.Client,
	runLog *logrus.Logger,
) *RdsReader {
	return &RdsReader{
		queue: queue,
		rds:    rds,
		runLog: runLog,
	}
}

func (r *RdsReader)ReadLine() (string, error) {
	return r.rds.LPop(r.queue).Result()
}