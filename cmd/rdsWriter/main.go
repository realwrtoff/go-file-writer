package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/hatlonely/go-kit/bind"
	"github.com/hatlonely/go-kit/config"
	"github.com/hatlonely/go-kit/flag"
	"github.com/hatlonely/go-kit/logger"
	"github.com/hatlonely/go-kit/refx"
	"github.com/realwrtoff/go-file-writer/internal/handler"
	"github.com/realwrtoff/go-file-writer/internal/parser"
	"github.com/realwrtoff/go-file-writer/internal/reader"
	"github.com/realwrtoff/go-file-writer/internal/scheduler"
	"github.com/realwrtoff/go-file-writer/internal/writer"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var Version string

type Options struct {
	flag.Options
	Service struct {
		RunType  string `flag:"--runType; usage: 运行类型"`
		FilePath string `flag:"--filePath; usage: 文件路径; default: ./data"`
		Offset   int    `flag:"--offset; usage: 偏移量; default: 0"`
		Num      int    `flag:"--num; usage: 并发数; default: 10"`
	}
	Redis struct {
		Addr     string `flag:"usage: redis addr"`
		Password string `flag:"usage: redis password"`
	}
	Logger struct {
		Run logger.Options
	}
}

func main() {
	var options Options
	refx.Must(flag.Struct(&options, refx.WithCamelName()))
	refx.Must(flag.Parse(flag.WithJsonVal()))
	if options.Help {
		fmt.Println(flag.Usage())
		return
	}
	if options.Version {
		fmt.Println(Version)
		return
	}

	if options.ConfigPath == "" {
		options.ConfigPath = "config/base.json"
	}
	cfg, err := config.NewConfigWithSimpleFile(options.ConfigPath)
	refx.Must(err)
	refx.Must(bind.Bind(&options, []bind.Getter{flag.Instance(),
		bind.NewEnvGetter(bind.WithEnvPrefix("GO")), cfg}, refx.WithCamelName()))

	runLog, err := logger.NewLoggerWithOptions(&options.Logger.Run, refx.WithCamelName())
	if err != nil {
		panic(err)
	}

	rds := redis.NewClient(&redis.Options{
		Addr:         options.Redis.Addr,
		Password:     options.Redis.Password,
		MaxRetries:   3,
		MinIdleConns: 1,
		PoolSize:     options.Service.Num,
	})
	if _, err := rds.Ping().Result(); err != nil {
		panic(err)
	}
	runLog.Infof("ping redis %v ok\n", options.Redis.Addr)

	ctx, cancel := context.WithCancel(context.Background())

	watcher := scheduler.NewWatcher(rds)
	go watcher.Run(ctx)

	var wg sync.WaitGroup //定义一个同步等待的组
	for i := 0; i < options.Service.Num; i++ {
		index := options.Service.Offset + i
		queue := fmt.Sprintf("%s:%d", options.Service.RunType, index)
		publisher := reader.NewRdsReader(queue, rds, runLog)
		analyzer := parser.NewParser(options.Service.RunType, runLog)
		filePrefix := fmt.Sprintf("%s/%s/%d_", options.Service.FilePath, options.Service.RunType, index)
		subscriber := writer.NewFileWriter(filePrefix, runLog)
		hd := handler.NewFrameHandler(index, options.Service.RunType, publisher, analyzer, subscriber, runLog)
		wg.Add(1)
		go hd.Run(&wg, ctx)
	}

	// graceful quit
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	runLog.Infof("%v shutdown ...", os.Args[0])

	cancel()
	wg.Wait()
	_ = rds.Close()
	_ = runLog.Close()
}
