package worker

import (
	"context"
	"testing"

	"github.com/ishaanjain1507/taskqueue/internal/models"
)

type mockQueue struct {
	enqueueErr error
	dequeueJob *models.Job
	dequeueErr error
}

func (m *mockQueue) Enqueue(ctx context.Context, job *models.Job) error {
	return m.enqueueErr
}
func (m *mockQueue) Dequeue(ctx context.Context) (*models.Job, error) {
	return m.dequeueJob, m.dequeueErr
}
func (m *mockQueue) SendToDead(ctx context.Context, job *models.Job) error { return nil }
func (m *mockQueue) QueueLength(ctx context.Context) (int64, int64, error) { return 0, 0, nil }

type mockStore struct {
	upsertErr error
}

func (m *mockStore) UpsertJob(job *models.Job) error {
	return m.upsertErr
}
func (m *mockStore) GetJob(id string) (*models.Job, error) { return nil, nil }
func (m *mockStore) ListJobsByStatus(status models.JobStatus, limit int) ([]models.Job, error) {
	return nil, nil
}
func (m *mockStore) CountByStatus() (map[string]int, error) { return nil, nil }

func TestNewPool(t *testing.T) {
	q := &mockQueue{}
	s := &mockStore{}

	pool := NewPool(q, s, 5)

	if pool.numWorkers != 5 {
		t.Errorf("expected 5 workers, got %d", pool.numWorkers)
	}
}
