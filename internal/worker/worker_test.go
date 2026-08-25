package worker

import (
	"context"
	"testing"

	"github.com/ishaanjain1507/taskqueue/internal/models"
)

type mockQueue struct {
	enqueueErr error
	dequeueJob *models.Delivery
	dequeueErr error
	ackCount   int
}

func (m *mockQueue) Enqueue(ctx context.Context, job *models.Job) error {
	return m.enqueueErr
}
func (m *mockQueue) Dequeue(ctx context.Context) (*models.Delivery, error) {
	return m.dequeueJob, m.dequeueErr
}
func (m *mockQueue) Ack(ctx context.Context, receipt string) error {
	m.ackCount++
	return nil
}
func (m *mockQueue) Requeue(ctx context.Context, receipt string, job *models.Job) error { return nil }
func (m *mockQueue) SendToDead(ctx context.Context, receipt string, job *models.Job) error {
	return nil
}
func (m *mockQueue) QueueLength(ctx context.Context) (int64, int64, error) { return 0, 0, nil }
func (m *mockQueue) Purge(ctx context.Context) error {
	return nil
}

type mockStore struct {
	upsertErr error
	getJob    *models.Job
}

func (m *mockStore) UpsertJob(job *models.Job) error {
	return m.upsertErr
}
func (m *mockStore) GetJob(id string) (*models.Job, error) { return m.getJob, nil }
func (m *mockStore) ListJobsByStatus(status models.JobStatus, limit int) ([]models.Job, error) {
	return nil, nil
}
func (m *mockStore) ListRecentJobs(limit int) ([]models.Job, error) {
	return nil, nil
}
func (m *mockStore) CountByStatus() (map[string]int, error) { return nil, nil }
func (m *mockStore) Purge() error                           { return nil }

func TestNewPool(t *testing.T) {
	q := &mockQueue{}
	s := &mockStore{}

	pool := NewPool(q, s, 5)

	if pool.numWorkers != 5 {
		t.Errorf("expected 5 workers, got %d", pool.numWorkers)
	}
}

func TestProcessJob_DoesNotAckWhenProcessingStateCannotPersist(t *testing.T) {
	q := &mockQueue{}
	s := &mockStore{upsertErr: context.DeadlineExceeded}
	pool := NewPool(q, s, 0)
	delivery := &models.Delivery{
		Job:     &models.Job{ID: "job-1", Type: "email_dispatch", MaxRetries: 3},
		Receipt: "1-0",
	}

	pool.processJob(context.Background(), 1, delivery)

	if q.ackCount != 0 {
		t.Fatal("job was acknowledged even though processing state was not persisted")
	}
}

func TestProcessJob_AcksAlreadyTerminalDelivery(t *testing.T) {
	terminal := &models.Job{ID: "job-1", Status: models.StatusSuccess}
	q := &mockQueue{}
	s := &mockStore{getJob: terminal}
	pool := NewPool(q, s, 0)
	delivery := &models.Delivery{Job: &models.Job{ID: "job-1"}, Receipt: "1-0"}

	pool.processJob(context.Background(), 1, delivery)

	if q.ackCount != 1 {
		t.Fatalf("expected terminal delivery to be acknowledged once, got %d", q.ackCount)
	}
}
