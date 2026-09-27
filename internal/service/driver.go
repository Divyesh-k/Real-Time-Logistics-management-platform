package service

import (
	"context"
	"errors"
	"real-time-logistics-management-platform/internal/model"
	"real-time-logistics-management-platform/internal/repository"
	"strings"
)

type DriverService struct {
	repository *repository.DriverRepository
}

func NewDriverService(repository *repository.DriverRepository) *DriverService {
	return &DriverService{
		repository: repository,
	}
}

func (s *DriverService) CreateDriver(ctx context.Context, driver *model.Driver) error {
	driver.Name = strings.TrimSpace(driver.Name)
	driver.Phone = strings.TrimSpace(driver.Phone)

	if driver.Name == "" {
		return errors.New("driver name is required")
	}

	if driver.Phone == "" {
		return errors.New("driver phone is required")
	}

	return s.repository.Create(ctx, driver)
}

func (s *DriverService) GetDriver(
	ctx context.Context,
	id string,
) (*model.Driver, error) {

	return s.repository.GetByID(ctx, id)
}

func (s *DriverService) ListDrivers(
	ctx context.Context,
) ([]model.Driver, error) {

	return s.repository.List(ctx)
}

func (s *DriverService) UpdateDriver(
	ctx context.Context,
	id string,
	name string,
	phone string,
	status string,
) (*model.Driver, error) {

	name = strings.TrimSpace(name)
	phone = strings.TrimSpace(phone)
	status = strings.TrimSpace(status)

	if name == "" {
		return nil, errors.New("driver name is required")
	}

	if phone == "" {
		return nil, errors.New("driver phone is required")
	}

	if status == "" {
		return nil, errors.New("driver status is required")
	}

	return s.repository.Update(
		ctx,
		id,
		name,
		phone,
		status,
	)
}

func (s *DriverService) DeleteDriver(
	ctx context.Context,
	id string,
) error {

	return s.repository.Delete(ctx, id)
}
