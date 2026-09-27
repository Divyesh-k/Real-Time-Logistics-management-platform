package config

import "os"

type Config struct {
	Port        string
	DatabaseURL string
	RedisURL    string
	JWTSecret   string
	KafkaURL    string
}

func Load() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	databaseUrl := os.Getenv("DATABASE_URL")

	return Config{
		Port:        port,
		DatabaseURL: databaseUrl,
		RedisURL:    os.Getenv("REDIS_URL"),
		JWTSecret:   os.Getenv("JWT_SECRET"),
		KafkaURL:    os.Getenv("KafkaURL"),
	}
}
