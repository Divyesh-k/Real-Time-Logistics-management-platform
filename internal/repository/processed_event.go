package repository

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ProcessedEventRepository struct {
	db *pgxpool.Pool
}

func NewProcessedEventRepository(
	db *pgxpool.Pool,
) *ProcessedEventRepository {
	return &ProcessedEventRepository{
		db: db,
	}
}

func (r *ProcessedEventRepository) TryMarkProcessed(ctx context.Context, eventId string) (bool, error) {
	query := `
        INSERT INTO processed_events (event_id)
        VALUES ($1)
        ON CONFLICT (event_id) DO NOTHING
    `

	result, err := r.db.Exec(
		ctx,
		query,
		eventId,
	)

	if err != nil {
		return false, err
	}

	return result.RowsAffected() == 1, nil
}
