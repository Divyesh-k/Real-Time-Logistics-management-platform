package middleware

import "net/http"

func RequireRole(allowedRoles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			role, ok := r.Context().Value(RoleKey).(string)

			if !ok {
				http.Error(
					w,
					"unauthorized",
					http.StatusUnauthorized,
				)
				return
			}

			for _, allowedRole := range allowedRoles {
				if role == allowedRole {
					next.ServeHTTP(w, r)
					return
				}
			}

			http.Error(
				w,
				"forbidden",
				http.StatusForbidden,
			)
		})
	}
}
