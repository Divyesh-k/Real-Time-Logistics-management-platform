package service

import (
	"context"
	"encoding/json"
	"fmt"
	"real-time-logistics-management-platform/internal/kafka"
	"real-time-logistics-management-platform/internal/repository"

	"github.com/twmb/franz-go/pkg/kgo"
)

type DeliveryStatusWorker struct {
	deliveryRepository *repository.DeliveryRepository
}

func NewDeliveryStatusWorker(
	deliveryRepository *repository.DeliveryRepository,
) *DeliveryStatusWorker {
	return &DeliveryStatusWorker{
		deliveryRepository: deliveryRepository,
	}
}

func (w *DeliveryStatusWorker) Process(
	ctx context.Context,
	event kafka.DeliveryStatusChangedEvent,
) error {

	if event.EventID == "" {
		return fmt.Errorf("event ID is required")
	}

	if event.DeliveryID == "" {
		return fmt.Errorf("delivery ID is required")
	}

	if event.Status == "" {
		return fmt.Errorf("delivery status is required")
	}

	processed, err := w.deliveryRepository.ProcessStatusChange(
		ctx,
		event.EventID,
		event.DeliveryID,
		event.Status,
	)

	if err != nil {
		return err
	}

	if !processed {
		// Duplicate event.
		// This is not an error.
		return nil
	}

	return nil
}

func NewDeliveryStatusProcessor(
	worker *DeliveryStatusWorker,
) func(context.Context, *kgo.Record) error {

	return func(
		ctx context.Context,
		record *kgo.Record,
	) error {

		var event kafka.DeliveryStatusChangedEvent

		if err := json.Unmarshal(
			record.Value,
			&event,
		); err != nil {
			return fmt.Errorf(
				"decode delivery status event: %w",
				err,
			)
		}

		return worker.Process(ctx, event)
	}
}
