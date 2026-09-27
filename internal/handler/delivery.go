package handler

import (
	"encoding/json"
	"net/http"
	"real-time-logistics-management-platform/internal/model"
	"real-time-logistics-management-platform/internal/service"

	"github.com/go-chi/chi/v5"
)

type DeliveryHandler struct {
	service *service.DeliveryService
}

func NewDeliveryHandler(
	service *service.DeliveryService,
) *DeliveryHandler {
	return &DeliveryHandler{
		service: service,
	}
}

func (h *DeliveryHandler) Create(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request model.CreateDeliveryRequest

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	delivery := &model.Delivery{
		DriverID:       request.DriverID,
		PickupAddress:  request.PickupAddress,
		DropoffAddress: request.DropoffAddress,
	}

	if err := h.service.CreateDelivery(
		r.Context(),
		delivery,
	); err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	json.NewEncoder(w).Encode(delivery)
}

func (h *DeliveryHandler) GetByID(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := chi.URLParam(r, "id")

	delivery, err := h.service.GetDelivery(
		r.Context(),
		id,
	)

	if err != nil {
		http.Error(
			w,
			"delivery not found",
			http.StatusNotFound,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(delivery)
}
func (h *DeliveryHandler) List(
	w http.ResponseWriter,
	r *http.Request,
) {
	deliveries, err := h.service.ListDeliveries(
		r.Context(),
	)

	if err != nil {
		http.Error(
			w,
			"failed to get deliveries",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(deliveries)
}

func (h *DeliveryHandler) UpdateStatus(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := chi.URLParam(r, "id")

	var request model.UpdateDeliveryStatusRequest

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(
			w,
			"invalid JSON",
			http.StatusBadRequest,
		)
		return
	}

	delivery, err := h.service.UpdateStatus(
		r.Context(),
		id,
		request.Status,
	)

	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(delivery)
}
