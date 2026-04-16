package events

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/go-platform/kafka"
	"github.com/mibrgmv/go-platform/outbox"
	"github.com/mibrgmv/go-platform/postgres"
	"github.com/mibrgmv/go-platform/workerpool"
)

type PublisherConfig struct {
	BatchSize         int           `yaml:"batch_size"`
	WorkerCount       int           `yaml:"worker_count"`
	ProcessInterval   time.Duration `yaml:"process_interval"`
	RetryInterval     time.Duration `yaml:"retry_interval"`
	CleanupInterval   time.Duration `yaml:"cleanup_interval"`
	CleanupDays       int           `yaml:"cleanup_days"`
	BaseRetryDelay    time.Duration `yaml:"base_retry_delay"`
	MaxRetryBackoff   time.Duration `yaml:"max_retry_backoff"`
	RetryJitterFactor float64       `yaml:"retry_jitter_factor"`
}

type Publisher struct {
	registry      *PublisherRegistry
	outboxRepo    outbox.Repository
	kafkaProducer kafka.Producer
	db            *postgres.DB
	config        PublisherConfig
	retryCalc     *retryCalculator
}

func NewPublisher(
	registry *PublisherRegistry,
	outboxRepo outbox.Repository,
	kafkaProducer kafka.Producer,
	db *postgres.DB,
	config PublisherConfig,
) *Publisher {
	retryCalc := newRetryCalculator(
		config.BaseRetryDelay,
		config.MaxRetryBackoff,
		config.RetryJitterFactor,
	)

	return &Publisher{
		registry:      registry,
		outboxRepo:    outboxRepo,
		kafkaProducer: kafkaProducer,
		db:            db,
		config:        config,
		retryCalc:     retryCalc,
	}
}

func (p *Publisher) Start(ctx context.Context) {
	go p.publishLoop(ctx)
	go p.retryLoop(ctx)
	go p.cleanupLoop(ctx)
}

func (p *Publisher) publishLoop(ctx context.Context) {
	ticker := time.NewTicker(p.config.ProcessInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.ProcessBatch(ctx, p.outboxRepo.GetPendingEvents); err != nil {
				log.Printf("Error processing pending events: %v", err)
			}
		}
	}
}

func (p *Publisher) retryLoop(ctx context.Context) {
	ticker := time.NewTicker(p.config.RetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.ProcessBatch(ctx, p.outboxRepo.GetRetryEvents); err != nil {
				log.Printf("Error processing retry events: %v", err)
			}
		}
	}
}

func (p *Publisher) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(p.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.outboxRepo.CleanupOldEvents(ctx, p.config.CleanupDays); err != nil {
				log.Printf("Error cleaning up old events: %v", err)
			}
		}
	}
}

func (p *Publisher) ProcessBatch(ctx context.Context, getEventsFunc func(context.Context, int) ([]outbox.Event, error)) error {
	events, err := getEventsFunc(ctx, p.config.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to get events: %w", err)
	}
	if len(events) == 0 {
		return nil
	}

	workerCount := min(len(events), p.config.WorkerCount)
	pool := workerpool.New(ctx, workerCount)

	results := make([]<-chan error, len(events))
	for i, event := range events {
		event := event
		results[i] = pool.Submit(func() error {
			return p.ProcessSingle(ctx, event)
		})
	}

	successCount := 0
	for _, ch := range results {
		if ch == nil {
			continue
		}
		if err := <-ch; err != nil {
			log.Printf("Failed to process event: %v", err)
		} else {
			successCount++
		}
	}

	pool.Shutdown()
	log.Printf("Processed %d events from outbox (%d successful)", len(events), successCount)
	return nil
}

func (p *Publisher) ProcessSingle(ctx context.Context, event outbox.Event) error {
	err := p.db.WithTransaction(ctx, func(tx pgx.Tx) error {
		lockedEvent, err := p.outboxRepo.LockEventForProcessing(ctx, tx, event.EventID)
		if err != nil {
			return fmt.Errorf("failed to lock event: %w", err)
		}
		if lockedEvent == nil {
			return nil
		}

		handler, exists := p.registry.GetHandler(lockedEvent.EventType)
		if !exists {
			return fmt.Errorf("no handler registered for event type: %s", lockedEvent.EventType)
		}

		handlerErr := handler.HandleEvent(ctx, lockedEvent, p.kafkaProducer)
		if handlerErr != nil {
			nextRetryAt := p.retryCalc.calculateNextRetry(lockedEvent.RetryCount + 1)

			markErr := p.outboxRepo.MarkEventAsFailedTx(ctx, tx, event.EventID, handlerErr.Error(), nextRetryAt)
			if markErr != nil {
				return fmt.Errorf("failed to mark event as failed: %w", markErr)
			}

			log.Printf("Event %s failed after retries: %v", event.EventID, handlerErr)
			return nil
		}

		return p.outboxRepo.MarkEventAsPublishedTx(ctx, tx, event.EventID)
	})

	if err != nil {
		return fmt.Errorf("failed to process event %s: %w", event.EventID, err)
	}

	return nil
}
