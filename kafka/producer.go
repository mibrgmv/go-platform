package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(ctx context.Context, topic string, key string, value []byte) error
	Close() error
}

type ProducerConfig struct {
	Brokers  []string
	ClientID string
}

type producer struct {
	writer *kafka.Writer
}

func NewProducer(cfg ProducerConfig) Producer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           10 * time.Millisecond,
		BatchSize:              100,
		Async:                  false,
		AllowAutoTopicCreation: true,
		Logger:                 kafka.LoggerFunc(log.Printf),
		ErrorLogger:            kafka.LoggerFunc(log.Printf),
	}

	return &producer{writer: writer}
}

func (p *producer) Produce(ctx context.Context, topic string, key string, value []byte) error {
	message := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	}

	return p.writer.WriteMessages(ctx, message)
}

func (p *producer) Close() error {
	return p.writer.Close()
}
