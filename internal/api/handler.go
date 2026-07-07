package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ishaanjain1507/taskqueue/internal/models"
	"github.com/ishaanjain1507/taskqueue/internal/worker"
)

type Handler struct {
	queue models.Queue
	store models.Store
	pool  *worker.Pool
}

func NewHandler(q models.Queue, store models.Store, pool *worker.Pool) *Handler {
	return &Handler{queue: q, store: store, pool: pool}
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

	// CRITICAL: persist to DB first so a fast worker can't overwrite with stale data
	if err := h.store.UpsertJob(job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist job"})
		return
	}

	if err := h.queue.Enqueue(c.Request.Context(), job); err != nil {
		// Roll back DB state if enqueue fails
		job.Status = models.StatusFailed
		job.Error = "failed to enqueue"
		h.store.UpsertJob(job)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue job"})
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

// ListRecentJobs handles GET /jobs/recent
func (h *Handler) ListRecentJobs(c *gin.Context) {
	jobs, err := h.store.ListRecentJobs(15)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list recent jobs"})
		return
	}
	c.JSON(http.StatusOK, jobs)
}

// RetryJob handles POST /jobs/:id/retry
func (h *Handler) RetryJob(c *gin.Context) {
	id := c.Param("id")
	job, err := h.store.GetJob(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}

	if job.Status != models.StatusFailed && job.Status != models.StatusDead {
		c.JSON(http.StatusBadRequest, gin.H{"error": "only failed or dead jobs can be retried"})
		return
	}

	job.Status = models.StatusPending
	job.Retries = 0
	job.WorkerID = 0
	job.StartedAt = nil
	job.CompletedAt = nil
	job.Error = ""
	job.UpdatedAt = time.Now()

	// CRITICAL: Always persist to DB *before* enqueuing to prevent race conditions!
	if err := h.store.UpsertJob(job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save job to db"})
		return
	}

	if err := h.queue.Enqueue(c.Request.Context(), job); err != nil {
		// Attempt to mark as failed in DB if queue fails
		job.Status = models.StatusFailed
		job.Error = "failed to enqueue"
		h.store.UpsertJob(job)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue job"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "job retried successfully"})
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
		"active_workers":   h.pool.ActiveWorkers(),
	})
}

func (h *Handler) ScaleWorkers(c *gin.Context) {
	var req struct {
		Count int `json:"count"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Count < 0 || req.Count > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "count must be between 0 and 1000"})
		return
	}
	h.pool.Scale(req.Count)
	c.JSON(http.StatusOK, gin.H{"message": "scaled successfully", "workers": req.Count})
}

// PurgeSystem handles DELETE /jobs/purge
func (h *Handler) PurgeSystem(c *gin.Context) {
	if err := h.queue.Purge(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to purge redis queue"})
		return
	}
	if err := h.store.Purge(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to purge postgres store"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "system completely purged"})
}

func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}