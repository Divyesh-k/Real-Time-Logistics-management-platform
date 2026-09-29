package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"real-time-logistics-management-platform/internal/kafka"

	"github.com/twmb/franz-go/pkg/kgo"
)

func NewDriverLocationProcessor() func(
	context.Context,
	*kgo.Record,
) error {

	return func(
		ctx context.Context,
		record *kgo.Record,
	) error {

		var event kafka.DriverLocationUpdatedEvent

		if err := json.Unmarshal(
			record.Value,
			&event,
		); err != nil {
			return fmt.Errorf(
				"decode driver location event: %w",
				err,
			)
		}

		log.Printf(
			"processing location event: event=%s driver=%s lat=%f lon=%f",
			event.EventID,
			event.DriverID,
			event.Latitude,
			event.Longitude,
		)

		// Actual background business logic will go here later.

		return nil
	}
}
