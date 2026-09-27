package service

import (
	"context"
	"real-time-logistics-management-platform/internal/repository"
)

type AuthorizationService struct {
	driverRepository *repository.DriverRepository
}

func NewAuthorizationService(
	driverRepository *repository.DriverRepository,
) *AuthorizationService {
	return &AuthorizationService{
		driverRepository: driverRepository,
	}
}

func (s *AuthorizationService) CanAccessDriver(
	ctx context.Context,
	userID string,
	driverID string,
) (bool, error) {

	driver, err := s.driverRepository.GetByUserID(
		ctx,
		userID,
	)

	if err != nil {
		return false, err
	}

	if driver.ID != driverID {
		return false, nil
	}

	return true, nil
}
