package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxEvent struct {
	ID          string
	EventID     string
	Topic       string
	EventKey    string
	Payload     []byte
	PublishedAt *time.Time
	CreatedAt   time.Time
}

type OutboxRepository struct {
	db *pgxpool.Pool
}

func NewOutboxRepository(
	db *pgxpool.Pool,
) *OutboxRepository {
	return &OutboxRepository{
		db: db,
	}
}

func (r *OutboxRepository) Create(
	ctx context.Context,
	event *OutboxEvent,
) error {

	query := `
		INSERT INTO outbox_events (
			event_id,
			topic,
			event_key,
			payload
		)
		VALUES ($1, $2, $3, $4)
	`

	_, err := r.db.Exec(
		ctx,
		query,
		event.EventID,
		event.Topic,
		event.EventKey,
		event.Payload,
	)

	return err
}

func (r *OutboxRepository) GetUnpublished(
	ctx context.Context,
	limit int,
) ([]OutboxEvent, error) {
	query := `
		SELECT
			id,
			event_id,
			topic,
			event_key,
			payload,
			published_at,
			created_at
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT $1`

	rows, err := r.db.Query(
		ctx, query, limit,
	)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []OutboxEvent

	for rows.Next() {
		var event OutboxEvent

		err := rows.Scan(
			&event.ID,
			&event.EventID,
			&event.Topic,
			&event.EventKey,
			&event.Payload,
			&event.PublishedAt,
			&event.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (r *OutboxRepository) MarkPublished(
	ctx context.Context,
	id string,
) error {

	query := `
		UPDATE outbox_events
		SET published_at = NOW()
		WHERE id = $1
	`

	_, err := r.db.Exec(
		ctx,
		query,
		id,
	)

	return err
}
