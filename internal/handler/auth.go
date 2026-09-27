package handler

import (
	"encoding/json"
	"net/http"
	"real-time-logistics-management-platform/internal/model"
	"real-time-logistics-management-platform/internal/service"
	"strings"
)

type AuthHandler struct {
	service *service.AuthService
}

func NewAuthHandler(
	service *service.AuthService,
) *AuthHandler {
	return &AuthHandler{
		service: service,
	}
}

func (h *AuthHandler) Login(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request model.LoginRequest

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(
			w,
			"invalid request",
			http.StatusBadRequest,
		)
		return
	}

	request.Email = strings.TrimSpace(
		request.Email,
	)

	token, err := h.service.Login(
		r.Context(),
		request.Email,
		request.Password,
	)

	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusUnauthorized,
		)
		return
	}

	response := model.LoginResponse{
		Token: token,
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	json.NewEncoder(w).Encode(response)
}
