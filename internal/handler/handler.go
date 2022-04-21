package handler

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/realwrtoff/go-file-writer/internal/parser"
	"github.com/realwrtoff/go-file-writer/internal/reader"
	"github.com/realwrtoff/go-file-writer/internal/writer"
	"github.com/sirupsen/logrus"
	"sync"
)

type Frame struct {
	index int
	runType string
	filePath string
	rds *redis.Client
	publisher  *reader.RdsReader
	analyzer   *parser.Parser
	subscriber *writer.FileWriter
	runLog *logrus.Logger
}

func NewFrame(
	index int,
	runType string,
	filePath string,
	rds *redis.Client,
	runLog *logrus.Logger) *Frame {
	queue := fmt.Sprintf("%s:%d", runType, index)
	publisher := reader.NewRdsReader(queue, rds, runLog)
	analyzer := parser.NewParser(runType, runLog)
	filePrefix := fmt.Sprintf("%s/%s_%d", filePath, runType, index)
	subscriber := writer.NewFileWriter(filePrefix, runLog)
	return &Frame{
		index:      index,
		runType:    runType,
		filePath:   filePath,
		rds:        rds,
		publisher:  publisher,
		analyzer:   analyzer,
		subscriber: subscriber,
		runLog:     runLog,
	}
}

func (f *Frame) Run(wg *sync.WaitGroup, ctx context.Context)  {
	for {
		select {
		case <-ctx.Done():
			wg.Done()
			f.runLog.Infof("hander[%s][%d] exit...\n", f.runType, f.index)
			return
		default:
		}

		line, err := f.publisher.ReadLine()
		if err != nil {
			f.runLog.Error(err.Error())
			// time.Sleep(time.Second)
			continue
		}
		key, array, err := f.analyzer.Parse(line)
		if err != nil {
			f.runLog.Error(err.Error())
			continue
		}
		fileName := fmt.Sprintf("%s_%s.txt", key)
		err = f.subscriber.Write(fileName, array)
		if err != nil {
			f.runLog.Error(err.Error())
			// continue
		}
	}
}
