package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ishaanjain1507/taskqueue/internal/db"
	"github.com/ishaanjain1507/taskqueue/internal/models"
	"github.com/ishaanjain1507/taskqueue/internal/queue"
)

type Handler struct {
	queue *queue.RedisQueue
	store *db.PostgresStore
}

func NewHandler(q *queue.RedisQueue, store *db.PostgresStore) *Handler {
	return &Handler{queue: q, store: store}
}

func (h *Handler) CreateJob(c *gin.Context) {
	var req models.CreateJobRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	maxRetries := req.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	job := &models.Job{
		ID:         uuid.NewString(),
		Type:       req.Type,
		Payload:    req.Payload,
		Status:     models.StatusPending,
		Retries:    0,
		MaxRetries: maxRetries,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	if err := h.queue.Enqueue(c.Request.Context(), job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue job"})
		return
	}

	// persist initial state so it's queryable even before a worker picks it up
	if err := h.store.UpsertJob(job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist job"})
		return
	}

	c.JSON(http.StatusAccepted, models.JobResponse{
		ID:        job.ID,
		Type:      job.Type,
		Status:    job.Status,
		CreatedAt: job.CreatedAt,
		Message:   "Job accepted for processing",
	})
}

// GetJob handles GET /jobs/:id
func (h *Handler) GetJob(c *gin.Context) {
	id := c.Param("id")
	job, err := h.store.GetJob(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	c.JSON(http.StatusOK, job)
}

// ListJobs handles GET /jobs?status=failed
func (h *Handler) ListJobs(c *gin.Context) {
	statusParam := c.DefaultQuery("status", string(models.StatusSuccess))
	jobs, err := h.store.ListJobsByStatus(models.JobStatus(statusParam), 50)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list jobs"})
		return
	}
	c.JSON(http.StatusOK, jobs)
}

func (h *Handler) QueueStats(c *gin.Context) {
	mainLen, deadLen, err := h.queue.QueueLength(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get queue stats"})
		return
	}

	historicalCounts, err := h.store.CountByStatus()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get historical stats"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"pending_in_redis": mainLen,
		"dead_in_redis":    deadLen,
		"historical":       historicalCounts,
	})
}

func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}