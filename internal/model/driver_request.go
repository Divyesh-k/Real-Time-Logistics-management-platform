package model

type UpdateDriverRequest struct {
	Name   string `json:"name"`
	Phone  string `json:"phone"`
	Status string `json:"status"`
}

type UpdateDriverLocationRequest struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}
