package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/ishaanjain1507/taskqueue/internal/models"
)

type mockQueue struct {
	enqueueErr error
}

func (m *mockQueue) Enqueue(ctx context.Context, job *models.Job) error {
	return m.enqueueErr
}
func (m *mockQueue) Dequeue(ctx context.Context) (*models.Delivery, error)              { return nil, nil }
func (m *mockQueue) Ack(ctx context.Context, receipt string) error                      { return nil }
func (m *mockQueue) Requeue(ctx context.Context, receipt string, job *models.Job) error { return nil }
func (m *mockQueue) SendToDead(ctx context.Context, receipt string, job *models.Job) error {
	return nil
}
func (m *mockQueue) QueueLength(ctx context.Context) (int64, int64, error) { return 0, 0, nil }
func (m *mockQueue) Purge(ctx context.Context) error                       { return nil }

type mockStore struct {
	upsertErr error
	upserted  *models.Job
	getJobRes *models.Job
	getJobErr error
}

func (m *mockStore) UpsertJob(job *models.Job) error {
	copy := *job
	m.upserted = &copy
	return m.upsertErr
}
func (m *mockStore) GetJob(id string) (*models.Job, error) {
	return m.getJobRes, m.getJobErr
}
func (m *mockStore) ListJobsByStatus(status models.JobStatus, limit int) ([]models.Job, error) {
	return nil, nil
}
func (m *mockStore) ListRecentJobs(limit int) ([]models.Job, error) {
	return nil, nil
}
func (m *mockStore) CountByStatus() (map[string]int, error) {
	return nil, nil
}
func (m *mockStore) Purge() error { return nil }

func TestCreateJob_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	q := &mockQueue{}
	s := &mockStore{}
	handler := NewHandler(q, s, nil)

	router := gin.Default()
	router.POST("/jobs", handler.CreateJob)

	reqBody := `{"type":"test_job","payload":"{}"}`
	req, _ := http.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, w.Code)
	}

	var res models.JobResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if res.Type != "test_job" {
		t.Errorf("expected type test_job, got %s", res.Type)
	}
	if res.Status != models.StatusPending {
		t.Errorf("expected status pending, got %s", res.Status)
	}
}

func TestGetJob_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	q := &mockQueue{}
	s := &mockStore{
		getJobErr: errors.New("not found"),
	}
	handler := NewHandler(q, s, nil)

	router := gin.Default()
	router.GET("/jobs/:id", handler.GetJob)

	req, _ := http.NewRequest(http.MethodGet, "/jobs/123", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestCreateJob_AllowsExplicitZeroRetries(t *testing.T) {
	gin.SetMode(gin.TestMode)
	q := &mockQueue{}
	s := &mockStore{}
	handler := NewHandler(q, s, nil)
	router := gin.New()
	router.POST("/jobs", handler.CreateJob)

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(
		`{"type":"test_job","payload":"{}","max_retries":0}`,
	))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, w.Code)
	}
	if s.upserted == nil || s.upserted.MaxRetries != 0 {
		t.Fatal("expected explicit zero max retries to be preserved")
	}
}

func TestCreateJob_RejectsInvalidMaxRetries(t *testing.T) {
	gin.SetMode(gin.TestMode)
	q := &mockQueue{}
	s := &mockStore{}
	handler := NewHandler(q, s, nil)
	router := gin.New()
	router.POST("/jobs", handler.CreateJob)

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(
		`{"type":"test_job","payload":"{}","max_retries":-1}`,
	))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
	if s.upserted != nil {
		t.Fatal("invalid job must not be persisted")
	}
}
