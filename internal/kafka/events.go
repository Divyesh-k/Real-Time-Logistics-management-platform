package kafka

import "time"

type DriverLocationUpdatedEvent struct {
	EventID   string    `json:"event_id"`
	DriverID  string    `json:"driver_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	UpdatedAt time.Time `json:"updated_at"`
}

type DeliveryStatusChangedEvent struct {
	EventID    string    `json:"event_id"`
	DeliveryID string    `json:"delivery_id"`
	DriverID   string    `json:"driver_id"`
	Status     string    `json:"status"`
	ChangedAt  time.Time `json:"changed_at"`
}
