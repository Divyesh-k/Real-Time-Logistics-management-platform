package service

import (
	"context"
	"errors"
	"real-time-logistics-management-platform/internal/repository"
	"time"
)

type DriverLocationService struct {
	repository *repository.DriverLocationRepository
}

func NewDriverLocationService(
	repository *repository.DriverLocationRepository,
) *DriverLocationService {
	return &DriverLocationService{
		repository: repository,
	}
}

func (s *DriverLocationService) UpdateLocation(
	ctx context.Context,
	driverID string,
	latitude float64,
	longitude float64,
) error {

	if driverID == "" {
		return errors.New("driver_id is required")
	}

	if latitude < -90 || latitude > 90 {
		return errors.New("invalid latitude")
	}

	if longitude < -180 || longitude > 180 {
		return errors.New("invalid longitude")
	}

	location := &repository.DriverLocation{
		DriverID:  driverID,
		Latitude:  latitude,
		Longitude: longitude,
		UpdatedAt: time.Now().UTC(),
	}

	return s.repository.Set(ctx, location)
}

func (s *DriverLocationService) GetLocation(
	ctx context.Context,
	driverID string,
) (*repository.DriverLocation, error) {

	return s.repository.Get(ctx, driverID)
}
