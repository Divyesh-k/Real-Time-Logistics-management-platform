package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt"
)

type contextKey string

const (
	UserIDKey contextKey = "user_id"
	RoleKey   contextKey = "role"
)

func Auth(jwtSecret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			authHeader := r.Header.Get("Authorization")

			if authHeader == "" {
				http.Error(
					w,
					"missing authorization header",
					http.StatusUnauthorized,
				)
				return
			}

			parts := strings.SplitN(authHeader, " ", 2)

			if len(parts) != 2 || parts[0] != "Bearer" {
				http.Error(
					w,
					"invalid authorization header",
					http.StatusUnauthorized,
				)
				return
			}

			tokenString := parts[1]

			token, err := jwt.Parse(
				tokenString,
				func(token *jwt.Token) (interface{}, error) {

					return []byte(jwtSecret), nil
				},
			)

			if err != nil || !token.Valid {
				http.Error(
					w,
					"invalid token",
					http.StatusUnauthorized,
				)
				return
			}

			claims, ok := token.Claims.(jwt.MapClaims)

			if !ok {
				http.Error(
					w,
					"invalid token claims",
					http.StatusUnauthorized,
				)
				return
			}

			userID, ok := claims["sub"].(string)

			if !ok {
				http.Error(
					w,
					"invalid user id",
					http.StatusUnauthorized,
				)
				return
			}

			role, ok := claims["role"].(string)

			if !ok {
				http.Error(
					w,
					"invalid role",
					http.StatusUnauthorized,
				)
				return
			}

			ctx := context.WithValue(
				r.Context(),
				UserIDKey,
				userID,
			)

			ctx = context.WithValue(
				ctx,
				RoleKey,
				role,
			)

			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}
