package handler

import (
	"encoding/json"
	"net/http"
	"real-time-logistics-management-platform/internal/kafka"
	"real-time-logistics-management-platform/internal/model"
	"real-time-logistics-management-platform/internal/service"
	"real-time-logistics-management-platform/internal/websocket"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type DriverLocationHandler struct {
	service       *service.DriverLocationService
	hub           *websocket.Hub
	kafkaProducer *kafka.Producer
}

func NewDriverLocationHandler(
	service *service.DriverLocationService,
	hub *websocket.Hub,
	kafka *kafka.Producer,
) *DriverLocationHandler {
	return &DriverLocationHandler{
		service:       service,
		hub:           hub,
		kafkaProducer: kafka,
	}
}

func (h *DriverLocationHandler) Update(
	w http.ResponseWriter,
	r *http.Request,
) {
	driverID := chi.URLParam(r, "id")

	var request model.UpdateDriverLocationRequest

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(
			w,
			"invalid JSON",
			http.StatusBadRequest,
		)
		return
	}
	// redis update
	err := h.service.UpdateLocation(
		r.Context(),
		driverID,
		request.Latitude,
		request.Longitude,
	)

	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	// kafka update
	event := kafka.DriverLocationUpdatedEvent{
		EventID:   uuid.NewString(),
		DriverID:  driverID,
		Latitude:  request.Latitude,
		Longitude: request.Longitude,
		UpdatedAt: time.Now().UTC(),
	}

	event_data, err := json.Marshal(event)

	if err != nil {
		http.Error(
			w,
			"failed to create event",
			http.StatusInternalServerError,
		)
		return
	}

	err = h.kafkaProducer.Publish(
		r.Context(),
		"driver.location.updated",
		driverID,
		event_data,
	)

	if err != nil {
		http.Error(
			w,
			"failed to publish event",
			http.StatusInternalServerError,
		)
		return
	}

	//web socket update
	message := map[string]interface{}{
		"driver_id": driverID,
		"latitude":  request.Latitude,
		"longitude": request.Longitude,
	}

	data, err := json.Marshal(message)
	if err != nil {
		http.Error(w, "failed to create message", http.StatusInternalServerError)
		return
	}

	h.hub.Broadcast(data)

	w.WriteHeader(http.StatusNoContent)
}

func (h *DriverLocationHandler) Get(
	w http.ResponseWriter,
	r *http.Request,
) {
	driverID := chi.URLParam(r, "id")

	location, err := h.service.GetLocation(
		r.Context(),
		driverID,
	)

	if err != nil {
		http.Error(
			w,
			"location not found",
			http.StatusNotFound,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(location)
}
