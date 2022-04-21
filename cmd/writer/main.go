package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/hpifu/go-kit/hconf"
	"github.com/hpifu/go-kit/henv"
	"github.com/hpifu/go-kit/hflag"
	"github.com/hpifu/go-kit/hrule"
	"github.com/hpifu/go-kit/logger"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
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
	RunType string `hflag:"--type; usage: 运行类型" hrule:"atLeast 1"`
	FilePath string `hflag:"--path; usage: 文件路径; default: ./data"`
	Offset   int `hflag:"--offset; usage: 偏移量; default: 0"`
	Num   int `hflag:"--num; usage: 并发数; default: 10"`
	Redis struct {
		Addr     string `hflag:"usage: redis addr" hrule:"atLeast 10"`
		Password string `hflag:"usage: redis password"`
	}
	Logger struct {
		Run logger.Options
	}
}


func main() {
	version := hflag.Bool("v", false, "print current version")
	configfile := hflag.String("c", "configs/server.json", "config file path")
	if err := hflag.Bind(&Options{}); err != nil {
		panic(err)
	}
	if err := hflag.Parse(); err != nil {
		panic(err)
	}
	if *version {
		fmt.Println(AppVersion)
		os.Exit(0)
	}

	// load config
	options := &Options{}
	config, err := hconf.New("json", "local", *configfile)
	if err != nil {
		panic(err)
	}
	if err := config.Unmarshal(options); err != nil {
		panic(err)
	}
	if err := henv.NewHEnv("SERV").Unmarshal(options); err != nil {
		panic(err)
	}
	if err := hflag.Unmarshal(options); err != nil {
		panic(err)
	}
	if err := hrule.Evaluate(options); err != nil {
		panic(err)
	}

	runLog, err := logger.NewLogger(&options.Logger.Run)
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
	for i:=0; i< options.Num; i++ {
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
	_ = runLog.Out.(*rotatelogs.RotateLogs).Close()
}
