package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ishaanjain1507/taskqueue/internal/models"
	"github.com/ishaanjain1507/taskqueue/internal/queue"
)

type Handler struct {
	queue *queue.RedisQueue
}

func NewHandler(q *queue.RedisQueue) *Handler {
	return &Handler{queue: q}
}

// CreateJob handles POST /jobs
// Accepts a job request, builds a Job, pushes it to the queue,
// and immediately returns 202 Accepted — it does NOT wait for processing.
func (h *Handler) CreateJob(c *gin.Context) {
	var req models.CreateJobRequest

	// Gin binds the JSON body to req and validates the `binding:"required"` tags
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	maxRetries := req.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3 // sensible default
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

	// 202 Accepted = "I've got your request, I'm working on it, don't wait here"
	c.JSON(http.StatusAccepted, models.JobResponse{
		ID:        job.ID,
		Type:      job.Type,
		Status:    job.Status,
		CreatedAt: job.CreatedAt,
		Message:   "Job accepted for processing",
	})
}

// QueueStats handles GET /stats
func (h *Handler) QueueStats(c *gin.Context) {
	mainLen, deadLen, err := h.queue.QueueLength(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get queue stats"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"pending_jobs": mainLen,
		"dead_jobs":    deadLen,
	})
}

// HealthCheck handles GET /health
func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}