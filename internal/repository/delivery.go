package repository

import (
	"context"
	"real-time-logistics-management-platform/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DeliveryRepository struct {
	db *pgxpool.Pool
}

func NewDeliveryRepository(
	db *pgxpool.Pool,
) *DeliveryRepository {
	return &DeliveryRepository{
		db: db,
	}
}

func (r *DeliveryRepository) Create(
	ctx context.Context,
	delivery *model.Delivery,
) error {

	query := `
		INSERT INTO deliveries (
			driver_id,
			pickup_address,
			dropoff_address
		)
		VALUES ($1, $2, $3)
		RETURNING
			id,
			driver_id,
			pickup_address,
			dropoff_address,
			status,
			created_at,
			updated_at
	`

	return r.db.QueryRow(
		ctx,
		query,
		delivery.DriverID,
		delivery.PickupAddress,
		delivery.DropoffAddress,
	).Scan(
		&delivery.ID,
		&delivery.DriverID,
		&delivery.PickupAddress,
		&delivery.DropoffAddress,
		&delivery.Status,
		&delivery.CreatedAt,
		&delivery.UpdatedAt,
	)
}

func (r *DeliveryRepository) GetByID(
	ctx context.Context,
	id string,
) (*model.Delivery, error) {

	query := `
		SELECT
			id,
			driver_id,
			pickup_address,
			dropoff_address,
			status,
			created_at,
			updated_at
		FROM deliveries
		WHERE id = $1
	`

	var delivery model.Delivery

	err := r.db.QueryRow(
		ctx,
		query,
		id,
	).Scan(
		&delivery.ID,
		&delivery.DriverID,
		&delivery.PickupAddress,
		&delivery.DropoffAddress,
		&delivery.Status,
		&delivery.CreatedAt,
		&delivery.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &delivery, nil
}

func (r *DeliveryRepository) List(
	ctx context.Context,
) ([]model.Delivery, error) {

	query := `
		SELECT
			id,
			driver_id,
			pickup_address,
			dropoff_address,
			status,
			created_at,
			updated_at
		FROM deliveries
		ORDER BY created_at DESC
	`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deliveries []model.Delivery

	for rows.Next() {
		var delivery model.Delivery

		err := rows.Scan(
			&delivery.ID,
			&delivery.DriverID,
			&delivery.PickupAddress,
			&delivery.DropoffAddress,
			&delivery.Status,
			&delivery.CreatedAt,
			&delivery.UpdatedAt,
		)

		if err != nil {
			return nil, err
		}

		deliveries = append(deliveries, delivery)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return deliveries, nil
}

func (r *DeliveryRepository) UpdateStatus(
	ctx context.Context,
	id string,
	status string,
) (*model.Delivery, error) {

	query := `
		UPDATE deliveries
		SET
			status = $1,
			updated_at = NOW()
		WHERE id = $2
		RETURNING
			id,
			driver_id,
			pickup_address,
			dropoff_address,
			status,
			created_at,
			updated_at
	`

	var delivery model.Delivery

	err := r.db.QueryRow(
		ctx,
		query,
		status,
		id,
	).Scan(
		&delivery.ID,
		&delivery.DriverID,
		&delivery.PickupAddress,
		&delivery.DropoffAddress,
		&delivery.Status,
		&delivery.CreatedAt,
		&delivery.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &delivery, nil
}
