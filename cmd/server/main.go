package main

import (
	"context"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"real-time-logistics-management-platform/internal/config"
	"real-time-logistics-management-platform/internal/database"
	"real-time-logistics-management-platform/internal/handler"
	"real-time-logistics-management-platform/internal/kafka"
	"real-time-logistics-management-platform/internal/middleware"
	"real-time-logistics-management-platform/internal/redis"
	"real-time-logistics-management-platform/internal/repository"
	"real-time-logistics-management-platform/internal/service"
	"real-time-logistics-management-platform/internal/websocket"
)

func main() {

	// ============================================
	// Configuration
	// ============================================

	cfg := config.Load()

	if cfg.DatabaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	if cfg.RedisURL == "" {
		log.Fatal("REDIS_URL is required")
	}

	if cfg.JWTSecret == "" {
		log.Fatal("JWT_SECRET is required")
	}

	if cfg.KafkaURL == "" {
		log.Fatal("KAFKA_URL is required")
	}

	ctx := context.Background()

	// ============================================
	// PostgreSQL
	// ============================================

	db, err := database.NewPostgres(
		ctx,
		cfg.DatabaseURL,
	)

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	// ============================================
	// Redis
	// ============================================

	redisClient, err := redis.NewClient(
		ctx,
		cfg.RedisURL,
	)

	if err != nil {
		log.Fatal(err)
	}

	defer redisClient.Close()

	// ============================================
	// Kafka Producer
	// ============================================

	kafkaProducer, err := kafka.NewProducer(
		cfg.KafkaURL,
	)

	if err != nil {
		log.Fatal(err)
	}

	defer kafkaProducer.Close()

	// ============================================
	// Repositories
	// ============================================

	driverRepository :=
		repository.NewDriverRepository(db)

	deliveryRepository :=
		repository.NewDeliveryRepository(db)

	userRepository :=
		repository.NewUserRepository(db)

	driverLocationRepository :=
		repository.NewDriverLocationRepository(
			redisClient,
		)

	// ============================================
	// Services
	// ============================================

	driverService :=
		service.NewDriverService(
			driverRepository,
		)

	deliveryService :=
		service.NewDeliveryService(
			deliveryRepository,
		)

	driverLocationService :=
		service.NewDriverLocationService(
			driverLocationRepository,
		)

	authService :=
		service.NewAuthService(
			userRepository,
			cfg.JWTSecret,
		)

	authorizationService :=
		service.NewAuthorizationService(
			driverRepository,
		)

	// ============================================
	// One WebSocket Hub
	// ============================================

	hub := websocket.NewHub()

	// ============================================
	// Handlers
	// ============================================

	driverHandler :=
		handler.NewDriverHandler(
			driverService,
		)

	deliveryHandler :=
		handler.NewDeliveryHandler(
			deliveryService,
		)

	authHandler :=
		handler.NewAuthHandler(
			authService,
		)

	driverLocationHandler :=
		handler.NewDriverLocationHandler(
			driverLocationService,
			hub,
			kafkaProducer,
		)

	webSocketHandler :=
		handler.NewWebSocketHandler(
			hub,
		)

	// ============================================
	// Kafka Consumer
	// ============================================

	consumer, err := kafka.NewConsumer(
		cfg.KafkaURL,
		"fleetflow-workers",
		"driver.location.updated",
		kafkaProducer,
		kafka.DLQTopic,
	)

	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	// ============================================
	// Start Kafka Worker
	// ============================================

	locationProcessor := service.NewDriverLocationProcessor()

	go consumer.Start(
		ctx,
		locationProcessor,
	)

	// ============================================
	// HTTP Router
	// ============================================

	r := chi.NewRouter()

	// ============================================
	// Public routes
	// ============================================

	r.Get(
		"/healthz",
		func(w http.ResponseWriter, r *http.Request) {

			w.WriteHeader(
				http.StatusOK,
			)

			w.Write([]byte("ok"))
		},
	)

	r.Post(
		"/login",
		authHandler.Login,
	)

	// ============================================
	// Protected routes
	// ============================================

	r.Group(func(r chi.Router) {

		// JWT authentication
		r.Use(
			middleware.Auth(
				cfg.JWTSecret,
			),
		)

		// ------------------------------------------
		// WebSocket
		// ------------------------------------------

		r.Get(
			"/ws",
			webSocketHandler.Connect,
		)

		// ------------------------------------------
		// Drivers
		// ------------------------------------------

		r.Get(
			"/drivers",
			driverHandler.List,
		)

		r.Get(
			"/drivers/{id}",
			driverHandler.GetByID,
		)

		// Admin only
		r.With(
			middleware.RequireRole("ADMIN"),
		).Post(
			"/drivers",
			driverHandler.Create,
		)

		r.With(
			middleware.RequireRole("ADMIN"),
		).Patch(
			"/drivers/{id}",
			driverHandler.Update,
		)

		r.With(
			middleware.RequireRole("ADMIN"),
		).Delete(
			"/drivers/{id}",
			driverHandler.Delete,
		)

		// Driver/Admin can update location.
		// Driver must own the driver ID.
		r.With(
			middleware.RequireRole(
				"DRIVER",
				"ADMIN",
			),
			middleware.RequireDriverOwnership(
				authorizationService,
			),
		).Put(
			"/drivers/{id}/location",
			driverLocationHandler.Update,
		)

		r.Get(
			"/drivers/{id}/location",
			driverLocationHandler.Get,
		)

		// ------------------------------------------
		// Deliveries
		// ------------------------------------------

		r.With(
			middleware.RequireRole(
				"ADMIN",
				"DISPATCHER",
			),
		).Post(
			"/deliveries",
			deliveryHandler.Create,
		)

		r.Get(
			"/deliveries",
			deliveryHandler.List,
		)

		r.Get(
			"/deliveries/{id}",
			deliveryHandler.GetByID,
		)

		r.With(
			middleware.RequireRole(
				"ADMIN",
				"DISPATCHER",
			),
		).Patch(
			"/deliveries/{id}/status",
			deliveryHandler.UpdateStatus,
		)
	})

	// ============================================
	// Start HTTP server
	// ============================================

	port := cfg.Port

	if port == "" {
		port = "3000"
	}

	log.Printf(
		"server running on :%s",
		port,
	)

	if err := http.ListenAndServe(
		":"+port,
		r,
	); err != nil {

		log.Fatal(err)
	}
}
