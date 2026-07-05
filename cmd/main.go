package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/ishaanjain1507/taskqueue/internal/api"
	"github.com/ishaanjain1507/taskqueue/internal/db"
	"github.com/ishaanjain1507/taskqueue/internal/queue"
	"github.com/ishaanjain1507/taskqueue/internal/worker"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, using system environment variables")
	}

	redisURL := os.Getenv("REDIS_URL")
	postgresURL := os.Getenv("POSTGRES_URL")
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	workerCount, err := strconv.Atoi(os.Getenv("WORKER_COUNT"))
	if err != nil || workerCount == 0 {
		workerCount = 3
	}

	q, err := queue.NewRedisQueue(redisURL)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}
	log.Println("connected to redis successfully")

	store, err := db.NewPostgresStore(postgresURL)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	log.Println("connected to postgres successfully")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := worker.NewPool(q, store, workerCount)
	pool.Start(ctx)

	h := api.NewHandler(q, store)
	router := gin.Default()
	router.GET("/health", h.HealthCheck)
	router.GET("/stats", h.QueueStats)
	router.POST("/jobs", h.CreateJob)
	router.GET("/jobs/:id", h.GetJob)
	router.GET("/jobs", h.ListJobs)

	log.Printf("starting server on port %s with %d workers", port, workerCount)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}