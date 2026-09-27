package handler

import (
	"encoding/json"
	"net/http"
	"real-time-logistics-management-platform/internal/model"
	"real-time-logistics-management-platform/internal/service"

	"github.com/go-chi/chi/v5"
)

type DriverHandler struct {
	service *service.DriverService
}

func NewDriverHandler(service *service.DriverService) *DriverHandler {
	return &DriverHandler{
		service: service,
	}
}

func (h *DriverHandler) Create(w http.ResponseWriter, r *http.Request) {
	var driver model.Driver

	if err := json.NewDecoder(r.Body).Decode(&driver); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if err := h.service.CreateDriver(r.Context(), &driver); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	json.NewEncoder(w).Encode(driver)
}

func (h *DriverHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	driver, err := h.service.GetDriver(r.Context(), id)

	if err != nil {
		http.Error(
			w,
			"driver not found",
			http.StatusNotFound,
		)
		return
	}

	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(driver)
}

func (h *DriverHandler) List(
	w http.ResponseWriter,
	r *http.Request,
) {
	drivers, err := h.service.ListDrivers(
		r.Context(),
	)

	if err != nil {
		http.Error(
			w,
			"failed to get drivers",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(drivers)
}

func (h *DriverHandler) Update(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := chi.URLParam(r, "id")

	var request model.UpdateDriverRequest

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(
			w,
			"invalid JSON",
			http.StatusBadRequest,
		)
		return
	}

	driver, err := h.service.UpdateDriver(
		r.Context(),
		id,
		request.Name,
		request.Phone,
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

	json.NewEncoder(w).Encode(driver)
}

func (h *DriverHandler) Delete(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := chi.URLParam(r, "id")

	err := h.service.DeleteDriver(
		r.Context(),
		id,
	)

	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusNotFound,
		)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
