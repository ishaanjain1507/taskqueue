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
	ID          string     `json:"id" db:"id"`
	Type        string     `json:"type" db:"type"`
	Payload     string     `json:"payload" db:"payload"`
	Status      JobStatus  `json:"status" db:"status"`
	Retries     int        `json:"retries" db:"retries"`
	MaxRetries  int        `json:"max_retries" db:"max_retries"`
	WorkerID    int        `json:"worker_id" db:"worker_id"`
	Error       string     `json:"error,omitempty" db:"error"`
	StartedAt   *time.Time `json:"started_at" db:"started_at"`
	CompletedAt *time.Time `json:"completed_at" db:"completed_at"`
	CreatedAt   time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at" db:"updated_at"`
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