package repository

import (
	"context"
	"fmt"
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

func (r *DeliveryRepository) ProcessStatusChange(ctx context.Context, eventId string, deliveryID string, status string) (bool, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return false, err
	}

	defer tx.Rollback(ctx)

	insertEventQuery := `
		INSERT INTO processed_events (
			event_id
		)
		VALUES ($1)
		ON CONFLICT (event_id)
		DO NOTHING
	`

	result, err := tx.Exec(
		ctx,
		insertEventQuery,
		eventId,
	)

	if err != nil {
		return false, err
	}

	// Event already processed.
	if result.RowsAffected() == 0 {
		return false, nil
	}

	// -----------------------------------------
	// 2. Update delivery
	// -----------------------------------------

	updateDeliveryQuery := `
		UPDATE deliveries
		SET
			status = $1,
			updated_at = NOW()
		WHERE id = $2
	`

	result, err = tx.Exec(
		ctx,
		updateDeliveryQuery,
		status,
		deliveryID,
	)

	if err != nil {
		return false, err
	}

	// Delivery doesn't exist.
	if result.RowsAffected() == 0 {
		return false, fmt.Errorf(
			"delivery %s not found",
			deliveryID,
		)
	}

	// -----------------------------------------
	// 3. Commit transaction
	// -----------------------------------------

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
