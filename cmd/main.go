package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ishaanjain1507/taskqueue/internal/api"
	"github.com/ishaanjain1507/taskqueue/internal/db"
	"github.com/ishaanjain1507/taskqueue/internal/metrics"
	"github.com/ishaanjain1507/taskqueue/internal/queue"
	"github.com/ishaanjain1507/taskqueue/internal/worker"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	h := api.NewHandler(q, store, pool)
	router := gin.Default()

	// Apply Prometheus middleware
	router.Use(metrics.PrometheusMiddleware())

	// Expose Prometheus metrics
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Serve the static UI files
	router.Static("/static", "./web")
	router.StaticFile("/", "./web/index.html")
	router.GET("/config", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"grafana_url": serviceURL(os.Getenv("GRAFANA_URL"), "http://localhost:3000"),
		})
	})

	router.GET("/health", h.HealthCheck)
	router.GET("/stats", h.QueueStats)
	router.POST("/workers/scale", h.ScaleWorkers)
	router.POST("/jobs", h.CreateJob)
	router.DELETE("/jobs/purge", h.PurgeSystem)
	router.GET("/jobs/recent", h.ListRecentJobs)
	router.POST("/jobs/:id/retry", h.RetryJob)
	router.GET("/jobs/:id", h.GetJob)
	router.GET("/jobs", h.ListJobs)

	// Use http.Server for graceful shutdown instead of router.Run()
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	// Start HTTP server in a goroutine
	go func() {
		log.Printf("starting server on port %s with %d workers", port, workerCount)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed: %v", err)
		}
	}()

	// Block until we receive a shutdown signal
	<-ctx.Done()
	log.Println("shutdown signal received, draining in-flight jobs...")

	// Give workers up to 10 seconds to finish in-flight jobs
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
	log.Println("server stopped gracefully")
}

func serviceURL(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = fallback
	}
	if !strings.Contains(value, "://") {
		value = "https://" + value
	}
	return strings.TrimRight(value, "/")
}
