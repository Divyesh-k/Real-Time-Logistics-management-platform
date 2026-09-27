package repository

import (
	"context"
	"errors"
	"real-time-logistics-management-platform/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DriverRepository struct {
	db *pgxpool.Pool
}

func NewDriverRepository(db *pgxpool.Pool) *DriverRepository {
	return &DriverRepository{
		db: db,
	}
}

func (r *DriverRepository) Create(ctx context.Context, driver *model.Driver) error {
	query := `
		INSERT INTO drivers (name, phone)
		VALUES ($1, $2)
		RETURNING id, name, phone, status, created_at, updated_at
	`
	return r.db.QueryRow(ctx, query, driver.Name, driver.Phone).Scan(&driver.ID, &driver.Name, &driver.Phone, &driver.Status, &driver.CreatedAt, &driver.UpdatedAt)
}

func (r *DriverRepository) GetByID(ctx context.Context, id string) (*model.Driver, error) {
	query := `
		SELECT id, name, phone, status, created_at, updated_at
		FROM drivers
		WHERE id = $1
	`

	var driver model.Driver

	err := r.db.QueryRow(
		ctx,
		query,
		id,
	).Scan(
		&driver.ID,
		&driver.Name,
		&driver.Phone,
		&driver.Status,
		&driver.CreatedAt,
		&driver.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &driver, nil
}
func (r *DriverRepository) List(ctx context.Context) ([]model.Driver, error) {
	query := `
		SELECT id, name, phone, status, created_at, updated_at
		FROM drivers
		ORDER BY created_at DESC
	`

	rows, err := r.db.Query(ctx, query)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var drivers []model.Driver

	for rows.Next() {
		var driver model.Driver

		err := rows.Scan(
			&driver.ID,
			&driver.Name,
			&driver.Phone,
			&driver.Status,
			&driver.CreatedAt,
			&driver.UpdatedAt,
		)

		if err != nil {
			return nil, err
		}

		drivers = append(drivers, driver)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return drivers, nil
}

func (r *DriverRepository) Update(
	ctx context.Context,
	id string,
	name string,
	phone string,
	status string,
) (*model.Driver, error) {

	query := `
		UPDATE drivers
		SET
			name = $1,
			phone = $2,
			status = $3,
			updated_at = NOW()
		WHERE id = $4
		RETURNING id, name, phone, status, created_at, updated_at
	`

	var driver model.Driver

	err := r.db.QueryRow(
		ctx,
		query,
		name,
		phone,
		status,
		id,
	).Scan(
		&driver.ID,
		&driver.Name,
		&driver.Phone,
		&driver.Status,
		&driver.CreatedAt,
		&driver.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &driver, nil
}

func (r *DriverRepository) Delete(
	ctx context.Context,
	id string,
) error {

	query := `
		DELETE FROM drivers
		WHERE id = $1
	`

	result, err := r.db.Exec(
		ctx,
		query,
		id,
	)

	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return errors.New("driver not found")
	}

	return nil
}

func (r *DriverRepository) GetByUserID(
	ctx context.Context,
	userID string,
) (*model.Driver, error) {

	query := `
		SELECT
			id,
			name,
			phone,
			status,
			created_at,
			updated_at
		FROM drivers
		WHERE user_id = $1
	`

	var driver model.Driver

	err := r.db.QueryRow(
		ctx,
		query,
		userID,
	).Scan(
		&driver.ID,
		&driver.Name,
		&driver.Phone,
		&driver.Status,
		&driver.CreatedAt,
		&driver.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &driver, nil
}
