package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/hatlonely/go-kit/config"
	"github.com/hatlonely/go-kit/flag"
	"github.com/hatlonely/go-kit/logger"
	"github.com/hatlonely/go-kit/refx"
	"github.com/hpifu/go-kit/henv"
	"github.com/hpifu/go-kit/hrule"
	"github.com/realwrtoff/go-file-writer/internal/handler"
	"github.com/realwrtoff/go-file-writer/internal/scheduler"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// AppVersion name
var AppVersion = "unknown"

type Options struct {
	RunType  string `flag:"--type; usage: 运行类型"`
	FilePath string `flag:"--path; usage: 文件路径; default: ./data"`
	Offset   int    `flag:"--offset; usage: 偏移量; default: 0"`
	Num      int    `flag:"--num; usage: 并发数; default: 10"`
	Redis    struct {
		Addr     string `flag:"usage: redis addr"`
		Password string `flag:"usage: redis password"`
	}
	Logger struct {
		Run logger.Options
	}
}

func main() {
	version := flag.Bool("v", false, "print current version")
	configfile := flag.String("c", "configs/server.json", "config file path")
	if *version {
		fmt.Println(AppVersion)
		os.Exit(0)
	}

	// load config
	options := &Options{}
	cfg, err := config.NewConfigWithSimpleFile(*configfile)
	if err != nil {
		panic(err)
	}
	if err := cfg.Unmarshal(options, refx.WithCamelName()); err != nil {
		panic(err)
	}
	if err := henv.NewHEnv("SERV").Unmarshal(options); err != nil {
		panic(err)
	}

	if err := hrule.Evaluate(options); err != nil {
		panic(err)
	}
	if err := flag.Struct(options, refx.WithCamelName()); err != nil {
		panic(err)
	}
	if err := flag.Parse(); err != nil {
		panic(err)
	}

	runLog, err := logger.NewLoggerWithOptions(&options.Logger.Run, refx.WithCamelName())
	if err != nil {
		panic(err)
	}

	rds := redis.NewClient(&redis.Options{
		Addr:         options.Redis.Addr,
		Password:     options.Redis.Password,
		MaxRetries:   3,
		MinIdleConns: 1,
		PoolSize:     options.Num,
	})
	if _, err := rds.Ping().Result(); err != nil {
		panic(err)
	}
	runLog.Infof("ping redis %v ok\n", options.Redis.Addr)

	ctx, cancel := context.WithCancel(context.Background())

	watcher := scheduler.NewWatcher(rds)
	go watcher.Run(ctx)

	var wg sync.WaitGroup //定义一个同步等待的组
	for i := 0; i < options.Num; i++ {
		index := options.Offset + i
		hd := handler.NewFrame(index, options.RunType, options.FilePath, rds, runLog)
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
