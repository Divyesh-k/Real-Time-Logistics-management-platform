package worker

import (
	"context"
	"time"
)

func retry(ctx context.Context, maxAttempts int, fn func() error) error {
	var err error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = fn()

		if err == nil {
			return nil
		}

		if attempt == maxAttempts {
			break
		}

		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return err
}
