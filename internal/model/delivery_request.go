package model

type CreateDeliveryRequest struct {
	DriverID       string `json:"driver_id"`
	PickupAddress  string `json:"pickup_address"`
	DropoffAddress string `json:"dropoff_address"`
}

type UpdateDeliveryStatusRequest struct {
	Status string `json:"status"`
}
