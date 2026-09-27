package worker

import (
	"context"
	"log"
	"real-time-logistics-management-platform/internal/kafka"
	"real-time-logistics-management-platform/internal/repository"
)

type LocationWorker struct {
	processedEvents *repository.ProcessedEventRepository
}

func NewLocationWorker(
	processedEvents *repository.ProcessedEventRepository,
) *LocationWorker {
	return &LocationWorker{
		processedEvents: processedEvents,
	}
}

func (w *LocationWorker) Process(
	ctx context.Context,
	event kafka.DriverLocationUpdatedEvent,
) error {

	isNew, err := w.processedEvents.TryMarkProcessed(
		ctx,
		event.EventID,
	)

	if err != nil {
		return err
	}

	if !isNew {
		log.Printf(
			"event already processed: %s",
			event.EventID,
		)

		return nil
	}

	log.Printf(
		"processing event: %s",
		event.EventID,
	)

	// Business logic goes here.

	return nil
}
