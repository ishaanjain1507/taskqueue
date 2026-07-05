package models

import "time"

type JobStatus string

const (
    StatusPending    JobStatus = "PENDING"
    StatusProcessing JobStatus = "PROCESSING"
    StatusSuccess    JobStatus = "SUCCESS"
    StatusFailed     JobStatus = "FAILED"
    StatusDead       JobStatus = "DEAD" // Dead Letter Queue
)

type Job struct {
    ID          string    `json:"id"`
    Type        string    `json:"type"`        // e.g. "image_resize", "pdf_generate"
    Payload     string    `json:"payload"`     // JSON string of task-specific data
    Status      JobStatus `json:"status"`
    Retries     int       `json:"retries"`
    MaxRetries  int       `json:"max_retries"`
    Error       string    `json:"error,omitempty"`
    CreatedAt   time.Time `json:"created_at"`
    UpdatedAt   time.Time `json:"updated_at"`
}

// CreateJobRequest is what the client sends
type CreateJobRequest struct {
    Type       string `json:"type" binding:"required"`
    Payload    string `json:"payload" binding:"required"`
    MaxRetries int    `json:"max_retries"`
}

// JobResponse is what the API returns
type JobResponse struct {
    ID        string    `json:"id"`
    Type      string    `json:"type"`
    Status    JobStatus `json:"status"`
    CreatedAt time.Time `json:"created_at"`
    Message   string    `json:"message"`
}