package model

import "time"

type Delivery struct {
	ID             string    `json:"id"`
	DriverID       string    `json:"driver_id"`
	PickupAddress  string    `json:"pickup_address"`
	DropoffAddress string    `json:"dropoff_address"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
