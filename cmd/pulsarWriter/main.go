package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/hatlonely/go-kit/bind"
	"github.com/hatlonely/go-kit/config"
	"github.com/hatlonely/go-kit/flag"
	"github.com/hatlonely/go-kit/logger"
	"github.com/hatlonely/go-kit/refx"
	"github.com/realwrtoff/go-file-writer/internal/handler"
	"github.com/realwrtoff/go-file-writer/internal/parser"
	"github.com/realwrtoff/go-file-writer/internal/reader"
	"github.com/realwrtoff/go-file-writer/internal/writer"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var Version string

type Options struct {
	flag.Options

	RunType  string `flag:"--type; usage: 运行类型"`
	FilePath string `flag:"--path; usage: 文件路径; default: ./data"`
	Offset   int    `flag:"--offset; usage: 偏移量; default: 0"`
	Num      int    `flag:"--num; usage: 并发数; default: 10"`

	Pulsar struct {
		URL string `flag:"usage: pulsar url"`
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

	ctx, cancel := context.WithCancel(context.Background())

	//定义一个同步等待的组
	var wg sync.WaitGroup
	for i := 0; i < options.Num; i++ {
		index := options.Offset + i
		client, err := pulsar.NewClient(pulsar.ClientOptions{URL: options.Pulsar.URL})
		if err != nil {
			panic(err)
		}
		topic := fmt.Sprintf("%s:%d", options.RunType, index)
		publisher := reader.NewPulsarReader(topic, client, runLog)
		analyzer := parser.NewParser(options.RunType, runLog)
		subscriber := writer.NewFileWriter(options.FilePath, runLog)
		hd := handler.NewFrameHandler(index, options.RunType, publisher, analyzer, subscriber, runLog)
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
	_ = runLog.Close()
}
