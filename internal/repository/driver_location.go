package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type DriverLocation struct {
	DriverID  string    `json:"driver_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	UpdatedAt time.Time `json:"updated_at"`
}

type DriverLocationRepository struct {
	redis *goredis.Client
}

func NewDriverLocationRepository(
	redis *goredis.Client,
) *DriverLocationRepository {
	return &DriverLocationRepository{
		redis: redis,
	}
}

func (r *DriverLocationRepository) Set(ctx context.Context, location *DriverLocation) error {
	key := fmt.Sprintf(
		"driver:location:%s",
		location.DriverID,
	)

	data, err := json.Marshal(location)

	if err != nil {
		return err
	}

	return r.redis.Set(
		ctx,
		key,
		data,
		5*time.Minute,
	).Err()
}

func (r *DriverLocationRepository) Get(
	ctx context.Context,
	driverID string,
) (*DriverLocation, error) {

	key := fmt.Sprintf(
		"driver:location:%s",
		driverID,
	)

	data, err := r.redis.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}

	var location DriverLocation

	if err := json.Unmarshal(data, &location); err != nil {
		return nil, err
	}

	return &location, nil
}
