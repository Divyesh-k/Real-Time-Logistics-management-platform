package kafka

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const DLQTopic = "driver.location.updated.dlq"

type Consumer struct {
	client   *kgo.Client
	producer *Producer
	dlqTopic string
}

type EventProcessor func(context.Context, *kgo.Record) error

func NewConsumer(

	broker string,
	group string,
	topic string,
	producer *Producer,
	dlqTopic string,
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
		dlqTopic: dlqTopic,
	}, nil
}

func (c *Consumer) Start(
	ctx context.Context,
	process EventProcessor,
) {

	for {

		fetches := c.client.PollFetches(ctx)

		if fetches.IsClientClosed() {
			return
		}

		fetches.EachError(
			func(
				_ string, _ int32, err error,
			) {
				log.Printf(
					"kafka consumer error: %v",
					err,
				)
			},
		)

		var processed []*kgo.Record

		fetches.EachRecord(
			func(record *kgo.Record) {

				// -----------------------------------
				// Process with retry
				// -----------------------------------

				err := processWithRetry(
					ctx,
					process,
					record,
				)

				if err != nil {

					log.Printf(
						"event failed after retries: topic=%s partition=%d offset=%d error=%v",
						record.Topic,
						record.Partition,
						record.Offset,
						err,
					)

					// --------------------------------
					// Send to DLQ
					// --------------------------------

					if err := c.publishDLQ(
						ctx,
						record,
					); err != nil {

						log.Printf(
							"failed to publish to DLQ: %v",
							err,
						)

						// Don't commit.
						return
					}

					log.Printf(
						"event moved to DLQ: topic=%s offset=%d",
						record.Topic,
						record.Offset,
					)
				}

				// -----------------------------------
				// Mark Kafka message handled
				// -----------------------------------

				processed = append(
					processed,
					record,
				)
			},
		)

		// ---------------------------------------
		// Commit offsets
		// ---------------------------------------

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
	process EventProcessor,
	record *kgo.Record,
) error {

	var err error

	const maxAttempts = 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {

		err = process(
			ctx,
			record,
		)

		if err == nil {
			return nil
		}

		log.Printf(
			"processing failed: attempt=%d/%d error=%v",
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

	return c.producer.Publish(
		ctx,
		c.dlqTopic,
		string(record.Key),
		record.Value,
	)
}

func (c *Consumer) Close() {
	c.client.Close()
}
