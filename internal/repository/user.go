package repository

import (
	"context"
	"real-time-logistics-management-platform/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

type UserRepository struct {
	db *pgxpool.Pool
}

func NewUserRepository(db *pgxpool.Pool) *UserRepository {
	return &UserRepository{
		db: db,
	}
}

func (r *UserRepository) GetByEmail(
	ctx context.Context,
	email string,
) (*model.User, error) {

	query := `
		SELECT
			id,
			email,
			password_hash,
			role,
			created_at
		FROM users
		WHERE email = $1
	`

	var user model.User

	err := r.db.QueryRow(
		ctx,
		query,
		email,
	).Scan(
		&user.ID,
		&user.Email,
		&user.PasswordHash,
		&user.Role,
		&user.CreatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &user, nil
}
