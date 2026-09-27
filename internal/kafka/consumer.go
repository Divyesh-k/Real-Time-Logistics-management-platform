package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const DLQTopic = "driver.location.updated.dlq"

type Consumer struct {
	client   *kgo.Client
	producer *Producer
}

func NewConsumer(
	broker string,
	group string,
	topic string,
	producer *Producer,
) (*Consumer, error) {

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
	)

	if err != nil {
		return nil, err
	}

	return &Consumer{
		client:   client,
		producer: producer,
	}, nil
}

func (c *Consumer) Start(
	ctx context.Context,
	process func(
		context.Context,
		DriverLocationUpdatedEvent,
	) error,
) {

	for {

		fetches := c.client.PollFetches(ctx)

		if fetches.IsClientClosed() {
			return
		}

		fetches.EachError(
			func(s string, i int32, err error) {
				log.Printf(
					"kafka consumer error: %v",
					err,
				)
			},
		)

		var processed []*kgo.Record

		fetches.EachRecord(
			func(record *kgo.Record) {

				// ---------------------------------------
				// 1. Decode Kafka message
				// ---------------------------------------

				var event DriverLocationUpdatedEvent

				err := json.Unmarshal(
					record.Value,
					&event,
				)

				if err != nil {

					log.Printf(
						"failed to decode event: %v",
						err,
					)

					// Invalid message cannot be processed.
					// Send it directly to DLQ.
					if err := c.publishDLQ(
						ctx,
						record,
					); err != nil {

						log.Printf(
							"failed to publish message to DLQ: %v",
							err,
						)

						// IMPORTANT:
						// Don't commit if DLQ publishing failed.
						return
					}

					processed = append(
						processed,
						record,
					)

					return
				}

				// ---------------------------------------
				// 2. Process with retry
				// ---------------------------------------

				err = processWithRetry(
					ctx,
					process,
					event,
				)

				if err != nil {

					log.Printf(
						"event failed after retries: event=%s error=%v",
						event.EventID,
						err,
					)

					// -----------------------------------
					// 3. Send failed event to DLQ
					// -----------------------------------

					if err := c.publishDLQ(
						ctx,
						record,
					); err != nil {

						log.Printf(
							"failed to publish event to DLQ: %v",
							err,
						)

						// DLQ failed.
						// Do NOT commit the original message.
						return
					}

					log.Printf(
						"event moved to DLQ: event=%s",
						event.EventID,
					)
				}

				// ---------------------------------------
				// 4. Mark original message as processed
				// ---------------------------------------

				processed = append(
					processed,
					record,
				)
			},
		)

		// -------------------------------------------
		// 5. Commit offsets
		// -------------------------------------------

		if len(processed) > 0 {

			if err := c.client.CommitRecords(
				ctx,
				processed...,
			); err != nil {

				log.Printf(
					"failed to commit offsets: %v",
					err,
				)
			}
		}
	}
}

func processWithRetry(
	ctx context.Context,
	process func(
		context.Context,
		DriverLocationUpdatedEvent,
	) error,
	event DriverLocationUpdatedEvent,
) error {

	var err error

	const maxAttempts = 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {

		err = process(
			ctx,
			event,
		)

		if err == nil {
			return nil
		}

		log.Printf(
			"event processing failed: event=%s attempt=%d/%d error=%v",
			event.EventID,
			attempt,
			maxAttempts,
			err,
		)

		if attempt == maxAttempts {
			break
		}

		select {

		case <-time.After(time.Second):

			// Retry.

		case <-ctx.Done():

			return ctx.Err()
		}
	}

	return err
}

func (c *Consumer) publishDLQ(
	ctx context.Context,
	record *kgo.Record,
) error {

	log.Printf(
		"publishing message to DLQ: topic=%s",
		DLQTopic,
	)

	return c.producer.Publish(
		ctx,
		DLQTopic,
		string(record.Key),
		record.Value,
	)
}

func (c *Consumer) Close() {
	c.client.Close()
}
