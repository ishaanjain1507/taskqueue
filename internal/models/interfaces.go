package models

import "context"

type Store interface {
	UpsertJob(job *Job) error
	GetJob(id string) (*Job, error)
	ListJobsByStatus(status JobStatus, limit int) ([]*Job, error)
	CountByStatus() (map[JobStatus]int64, error)
}

type Queue interface {
	Enqueue(ctx context.Context, job *Job) error
	Dequeue(ctx context.Context) (*Job, error)
	SendToDead(ctx context.Context, job *Job) error
	QueueLength(ctx context.Context) (int64, int64, error)
}
