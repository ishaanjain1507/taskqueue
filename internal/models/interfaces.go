package models

import "context"

type Store interface {
	UpsertJob(job *Job) error
	GetJob(id string) (*Job, error)
	ListJobsByStatus(status JobStatus, limit int) ([]Job, error)
	ListRecentJobs(limit int) ([]Job, error)
	CountByStatus() (map[string]int, error)
	Purge() error
}

// Delivery is a job reserved by a worker. Receipt identifies the queue message
// that must be acknowledged only after the job's final state is durable.
type Delivery struct {
	Job     *Job
	Receipt string
}

type Queue interface {
	Enqueue(ctx context.Context, job *Job) error
	Dequeue(ctx context.Context) (*Delivery, error)
	Ack(ctx context.Context, receipt string) error
	Requeue(ctx context.Context, receipt string, job *Job) error
	SendToDead(ctx context.Context, receipt string, job *Job) error
	QueueLength(ctx context.Context) (int64, int64, error)
	Purge(ctx context.Context) error
}
