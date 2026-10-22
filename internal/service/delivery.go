package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"real-time-logistics-management-platform/internal/kafka"
	"real-time-logistics-management-platform/internal/model"
	"real-time-logistics-management-platform/internal/repository"
	"strings"
	"time"

	"github.com/google/uuid"
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
	deliveryID string,
	status string,
) (*model.Delivery, error) {

	if deliveryID == "" {
		return nil, fmt.Errorf("delivery ID is required")
	}

	status = strings.ToUpper(strings.TrimSpace(status))

	if isValidDeliveryStatus(status) {
		return nil, fmt.Errorf("Invalid delievery status")
	}

	eventID := uuid.NewString()

	event := &kafka.DeliveryStatusChangedEvent{
		EventID:    eventID,
		DeliveryID: deliveryID,
		Status:     status,
		ChangedAt:  time.Now().UTC(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to marshal delivery event: %w",
			err,
		)
	}

	outboxEvent := &repository.OutboxEvent{
		EventID:  eventID,
		Topic:    "delivery.status.changed",
		EventKey: deliveryID,
		Payload:  payload,
	}

	return s.repository.UpdateStatusWithOutbox(
		ctx,
		deliveryID,
		status,
		outboxEvent,
	)
}

func (s *DeliveryService) UpdateStatusWithOutbox(
	ctx context.Context,
	deliveryID string,
	status string,
) (*model.Delivery, error) {

	if deliveryID == "" {
		return nil, fmt.Errorf("delivery ID is required")
	}

	status = strings.ToUpper(strings.TrimSpace(status))

	if !isValidDeliveryStatus(status) {
		return nil, fmt.Errorf("invalid delivery status: %s", status)
	}

	eventID := uuid.NewString()

	event := kafka.DeliveryStatusChangedEvent{
		EventID:    eventID,
		DeliveryID: deliveryID,
		Status:     status,
		ChangedAt:  time.Now().UTC(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal delivery event: %w", err)
	}

	outboxEvent := &repository.OutboxEvent{
		EventID:  event.EventID,
		Topic:    "delivery.status.changed",
		EventKey: deliveryID,
		Payload:  payload,
	}

	delivery, err := s.repository.UpdateStatusWithOutbox(
		ctx,
		deliveryID,
		status,
		outboxEvent,
	)
	if err != nil {
		return nil, err
	}

	// We now know the actual driver ID from the database.
	// The event stored above should contain it too.
	return delivery, nil
}

func isValidDeliveryStatus(status string) bool {
	switch status {
	case "CREATED", "ASSIGNED", "PICKED_UP", "IN_TRANSIT", "DELIVERED", "CANCELLED":
		return true

	default:
		return false
	}
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
