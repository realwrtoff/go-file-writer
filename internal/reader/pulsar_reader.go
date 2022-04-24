package reader

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/hatlonely/go-kit/logger"
)

type PulsarReader struct {
	queue    string
	client   pulsar.Client
	consumer pulsar.Consumer
	runLog   *logger.Logger
}

func NewPulsarReader(
	queue string,
	client pulsar.Client,
	runLog *logger.Logger,
) *PulsarReader {
	runLog.Infof("New pulsar reader for topic[%s]\n", queue)
	consumer, _ := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            queue,
		SubscriptionName: "goSubConsumer",
		Type:             pulsar.Shared,
	})
	return &PulsarReader{
		queue:    queue,
		client:   client,
		consumer: consumer,
		runLog:   runLog,
	}
}

func (r *PulsarReader) ReadLine() (string, error) {
	msg, err := r.consumer.Receive(context.Background())
	r.consumer.Ack(msg)
	return string(msg.Payload()), err
}

func (r *PulsarReader) Close() {
	r.consumer.Close()
	r.client.Close()
}
