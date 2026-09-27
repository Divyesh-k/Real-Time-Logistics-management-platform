package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

func NewClient(ctx context.Context, redisURL string) (*goredis.Client, error) {
	client := goredis.NewClient(&goredis.Options{
		Addr: redisURL,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return client, nil
}
