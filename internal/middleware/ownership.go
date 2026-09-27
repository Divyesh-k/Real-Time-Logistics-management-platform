package middleware

import (
	"net/http"
	"real-time-logistics-management-platform/internal/service"

	"github.com/go-chi/chi/v5"
)

func RequireDriverOwnership(authorizationService *service.AuthorizationService) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			userId, ok := r.Context().Value(UserIDKey).(string)

			if !ok {
				http.Error(
					w,
					"unauthorized",
					http.StatusUnauthorized,
				)
				return
			}

			driverId := chi.URLParam(r, "id")

			is_allowed, err := authorizationService.CanAccessDriver(
				r.Context(),
				userId,
				driverId,
			)

			if err != nil {
				http.Error(
					w,
					"failed to check ownership",
					http.StatusInternalServerError,
				)
				return
			}

			if !is_allowed {
				http.Error(
					w,
					"forbidden",
					http.StatusForbidden,
				)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
