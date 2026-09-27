package service

import (
	"context"
	"errors"
	"real-time-logistics-management-platform/internal/model"
	"real-time-logistics-management-platform/internal/repository"
	"strings"
)

type DeliveryService struct {
	repository *repository.DeliveryRepository
}

func NewDeliveryService(
	repository *repository.DeliveryRepository,
) *DeliveryService {
	return &DeliveryService{
		repository: repository,
	}
}

func (s *DeliveryService) CreateDelivery(
	ctx context.Context,
	delivery *model.Delivery,
) error {

	delivery.DriverID = strings.TrimSpace(delivery.DriverID)
	delivery.PickupAddress = strings.TrimSpace(delivery.PickupAddress)
	delivery.DropoffAddress = strings.TrimSpace(delivery.DropoffAddress)

	if delivery.DriverID == "" {
		return errors.New("driver_id is required")
	}

	if delivery.PickupAddress == "" {
		return errors.New("pickup_address is required")
	}

	if delivery.DropoffAddress == "" {
		return errors.New("dropoff_address is required")
	}

	return s.repository.Create(ctx, delivery)
}

func (s *DeliveryService) GetDelivery(
	ctx context.Context,
	id string,
) (*model.Delivery, error) {

	return s.repository.GetByID(ctx, id)
}

func (s *DeliveryService) ListDeliveries(
	ctx context.Context,
) ([]model.Delivery, error) {

	return s.repository.List(ctx)
}

func (s *DeliveryService) UpdateStatus(
	ctx context.Context,
	id string,
	status string,
) (*model.Delivery, error) {

	status = strings.ToUpper(strings.TrimSpace(status))

	switch status {
	case "CREATED", "ASSIGNED", "PICKED_UP", "IN_TRANSIT", "DELIVERED", "CANCELLED":
	default:
		return nil, errors.New("invalid delivery status")
	}

	return s.repository.UpdateStatus(
		ctx,
		id,
		status,
	)
}

func (s *DriverService) IsOwner(
	ctx context.Context,
	userID string,
	driverID string,
) (bool, error) {

	driver, err := s.repository.GetByUserID(
		ctx,
		userID,
	)

	if err != nil {
		return false, err
	}

	return driver.ID == driverID, nil
}
