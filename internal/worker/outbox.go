package worker

import (
	"context"
	"log"
	"real-time-logistics-management-platform/internal/kafka"
	"real-time-logistics-management-platform/internal/repository"
	"time"
)

type OutboxWorker struct {
	repository *repository.OutboxRepository
	producer   *kafka.Producer
}

func NewOutboxWorker(
	repository *repository.OutboxRepository,
	producer *kafka.Producer,
) *OutboxWorker {
	return &OutboxWorker{
		repository: repository,
		producer:   producer,
	}
}

func (w *OutboxWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.process(ctx)
		}

	}
}

func (w *OutboxWorker) process(ctx context.Context) {
	events, err := w.repository.GetUnpublished(
		ctx,
		100,
	)

	if err != nil {
		log.Printf(
			"failed to get outbox events: %v",
			err,
		)
		return
	}
	for _, event := range events {
		err := w.producer.Publish(
			ctx,
			event.Topic,
			event.EventKey,
			event.Payload,
		)

		if err != nil {
			log.Printf(
				"failed to publish outbox event %s: %v",
				event.EventID,
				err,
			)

			continue
		}

		err = w.repository.MarkPublished(
			ctx,
			event.ID,
		)

		if err != nil {
			log.Printf(
				"failed to mark event %s as published: %v",
				event.EventID,
				err,
			)

			continue
		}

		log.Printf(
			"outbox event published: event=%s topic=%s",
			event.EventID,
			event.Topic,
		)
	}
}
