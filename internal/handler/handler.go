package handler

import (
	"context"
	"github.com/hatlonely/go-kit/logger"
	"github.com/realwrtoff/go-file-writer/internal/parser"
	"github.com/realwrtoff/go-file-writer/internal/reader"
	"github.com/realwrtoff/go-file-writer/internal/writer"
	"sync"
	"time"
)

type FrameHandler struct {
	index      int
	runType    string
	publisher  reader.InterfaceReader
	analyzer   parser.InterfaceParser
	subscriber writer.InterfaceWriter
	runLog     *logger.Logger
}

func NewFrameHandler(
	index int,
	runType string,
	publisher reader.InterfaceReader,
	analyzer parser.InterfaceParser,
	subscriber writer.InterfaceWriter,
	runLog *logger.Logger) *FrameHandler {
	return &FrameHandler{
		index:      index,
		runType:    runType,
		publisher:  publisher,
		analyzer:   analyzer,
		subscriber: subscriber,
		runLog:     runLog,
	}
}

func (f *FrameHandler) Run(wg *sync.WaitGroup, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			f.publisher.Close()
			f.analyzer.Close()
			f.subscriber.Close()
			f.runLog.Infof("hander[%s][%d] exit...", f.runType, f.index)
			wg.Done()
			return
		default:
		}

		line, err := f.publisher.ReadLine()
		if err != nil {
			f.runLog.Error(err.Error())
			continue
		}
		// 取到的消息为空
		if len(line) == 0 {
			time.Sleep(time.Second)
			continue
		}
		keyArray, buffArray, err := f.analyzer.Parse(line)
		if err != nil {
			f.runLog.Error(err.Error())
			continue
		}
		// 解析的信息有误，直接丢弃
		if len(buffArray) == 0 {
			continue
		}
		err = f.subscriber.Write(keyArray, buffArray)
		if err != nil {
			f.runLog.Error(err.Error())
			// continue
		}
	}
}
