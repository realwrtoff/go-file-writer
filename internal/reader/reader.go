package reader

import (
	"github.com/go-redis/redis"
	"github.com/hatlonely/go-kit/logger"
)

type InterfaceReader interface {
	ReadLine() (string, error)
	Close()
}

type RdsReader struct {
	queue  string
	rds    *redis.Client
	runLog *logger.Logger
}

func NewRdsReader(
	queue string,
	rds *redis.Client,
	runLog *logger.Logger,
) *RdsReader {
	runLog.Infof("New redis reader for queue[%s]\n", queue)
	return &RdsReader{
		queue:  queue,
		rds:    rds,
		runLog: runLog,
	}
}

func (r *RdsReader) ReadLine() (string, error) {
	msg, err := r.rds.LPop(r.queue).Result()
	if err == redis.Nil {
		return "", err
	}
	return msg, err
}

func (r *RdsReader) Close() {
	// rds 在外面关闭
}
