package service

import (
	"context"
	"errors"
	"real-time-logistics-management-platform/internal/repository"
	"time"

	"github.com/golang-jwt/jwt"
	"golang.org/x/crypto/bcrypt"
)

type AuthService struct {
	userRepository *repository.UserRepository
	jwtToken       string
}

func NewAuthService(
	userRepository *repository.UserRepository,
	token string,
) *AuthService {
	return &AuthService{
		userRepository: userRepository,
		jwtToken:       token,
	}
}

func (s *AuthService) Login(
	ctx context.Context,
	email string,
	password string,
) (string, error) {
	user, err := s.userRepository.GetByEmail(ctx, email)

	if err != nil {
		return "", errors.New("invalid email or password")
	}

	err = bcrypt.CompareHashAndPassword(
		[]byte(user.PasswordHash),
		[]byte(password),
	)

	if err != nil {
		return "", errors.New("invalid email or password")
	}

	claims := jwt.MapClaims{
		"sub":  user.ID,
		"role": user.Role,
		"exp":  time.Now().Add(24 * time.Hour).Unix(),
	}

	token := jwt.NewWithClaims(
		jwt.SigningMethodHS256,
		claims,
	)

	signedToken, err := token.SignedString(
		[]byte(s.jwtToken),
	)

	if err != nil {
		return "", err
	}

	return signedToken, nil
}
