package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/hatlonely/go-kit/flag"
	"github.com/hatlonely/go-kit/refx"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Options struct {

	// Pika主机
	URL  string `flag:"--url; usage: pulsar url;"`
	Type string `flag:"--type; usage: source type; default tuvssh"`

	// 处理配置
	Offset int    `flag:"--offset; usage: 偏移量; default: 0"`
	Num    int    `flag:"--num; usage: 并发数; default: 1"`
	Split  string `flag:"--split; usage: 分隔符;"`
}

func Run(topic string, client *pulsar.Client, wg *sync.WaitGroup, ctx context.Context) {
	consumer, err := (*client).Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "goConsumer",
		Type:             pulsar.Shared,
	})
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	fmt.Sprintf("New consumer for topic[%s]\n", topic)

	receiveCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			_ = consumer.Unsubscribe()
			consumer.Close()
			fmt.Printf("consumer topic[%s] exit...\n", topic)
			wg.Done()
			return
		default:
		}

		msg, err := consumer.Receive(receiveCtx)
		if err != nil {
			if err != context.DeadlineExceeded {
				fmt.Println(err.Error())
			}
			continue
		}

		fmt.Printf("Topic[%s] received message: [%s]\n", topic, string(msg.Payload()))

		consumer.Ack(msg)
	}
}

func main() {
	if err := flag.Struct(&Options{}, refx.WithCamelName()); err != nil {
		panic(err)
	}
	if err := flag.Parse(); err != nil {
		fmt.Println(flag.Usage())
		return
	}

	url := flag.GetString("url")
	offset := flag.GetInt("offset")
	num := flag.GetInt("num")
	split := flag.GetStringD("split", ",")
	dataType := flag.GetString("type")
	fmt.Printf("数据类型[%s]的分隔符split是[%s]\n", dataType, split)

	// 获取任务文件

	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: url})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	//定义一个同步等待的组
	var wg sync.WaitGroup
	for i := 0; i < num; i++ {
		index := offset + i
		topic := fmt.Sprintf("%s:%d", dataType, index)
		wg.Add(1)
		go Run(topic, &pulsarClient, &wg, ctx)
	}

	// graceful quit
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Printf("%v shutdown ...\n", os.Args[0])

	cancel()
	wg.Wait()

	wg.Wait()
	pulsarClient.Close()
}
