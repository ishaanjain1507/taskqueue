package db

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sony/gobreaker"
	"github.com/ishaanjain1507/taskqueue/internal/models"
)

type PostgresStore struct {
	db *sqlx.DB
	cb *gobreaker.CircuitBreaker
}

func NewPostgresStore(connStr string) (*PostgresStore, error) {
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	// Size the pool to handle concurrent worker UpsertJob calls
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)

	// Circuit breaker to protect Postgres from cascading failures
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "Postgres",
		MaxRequests: 5,
		Interval:    10 * time.Second,
		Timeout:     5 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 10 && failureRatio >= 0.6
		},
	})

	store := &PostgresStore{
		db: db,
		cb: cb,
	}
	if err := store.migrate(); err != nil {
		return nil, fmt.Errorf("failed to migrate: %w", err)
	}

	return store, nil
}

// migrate creates the jobs table if it doesn't exist.
// In a real production system you'd use a proper migration tool
// (golang-migrate, goose) — this is a simplified version for the project.
func (s *PostgresStore) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id UUID PRIMARY KEY,
		type VARCHAR(100) NOT NULL,
		payload TEXT,
		status VARCHAR(20) NOT NULL,
		retries INT NOT NULL DEFAULT 0,
		max_retries INT NOT NULL DEFAULT 3,
		error TEXT,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		started_at TIMESTAMPTZ,
		completed_at TIMESTAMPTZ,
		worker_id INT
	);
	CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
	CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
	`
	_, err := s.db.Exec(schema)
	if err != nil {
		return err
	}
	
	// Safe schema migration for existing DBs
	s.db.Exec(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;`)
	s.db.Exec(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;`)
	s.db.Exec(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS worker_id INT;`)
	
	return nil
}

// UpsertJob inserts a new job or updates it if the ID already exists.
// This is called at every state transition: created, processing, success, failed, dead.
func (s *PostgresStore) UpsertJob(job *models.Job) error {
	query := `
	INSERT INTO jobs (id, type, payload, status, retries, max_retries, error, started_at, completed_at, created_at, updated_at, worker_id)
	VALUES (:id, :type, :payload, :status, :retries, :max_retries, :error, :started_at, :completed_at, :created_at, :updated_at, :worker_id)
	ON CONFLICT (id) DO UPDATE SET
		status = EXCLUDED.status,
		retries = EXCLUDED.retries,
		error = EXCLUDED.error,
		started_at = EXCLUDED.started_at,
		completed_at = EXCLUDED.completed_at,
		worker_id = EXCLUDED.worker_id,
		updated_at = EXCLUDED.updated_at
	`
	
	_, err := s.cb.Execute(func() (interface{}, error) {
		_, execErr := s.db.NamedExec(query, job)
		return nil, execErr
	})
	
	return err
}

// GetJob fetches a single job by ID — powers a future GET /jobs/{id} endpoint
func (s *PostgresStore) GetJob(id string) (*models.Job, error) {
	var job models.Job
	err := s.db.Get(&job, "SELECT * FROM jobs WHERE id = $1", id)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// ListJobsByStatus — powers "show me all failed jobs" type queries
func (s *PostgresStore) ListJobsByStatus(status models.JobStatus, limit int) ([]models.Job, error) {
	var jobs []models.Job
	err := s.db.Select(&jobs,
		"SELECT * FROM jobs WHERE status = $1 ORDER BY created_at DESC LIMIT $2",
		status, limit,
	)
	return jobs, err
}

// ListRecentJobs fetches the most recent jobs regardless of status
func (s *PostgresStore) ListRecentJobs(limit int) ([]models.Job, error) {
	var jobs []models.Job
	err := s.db.Select(&jobs,
		"SELECT * FROM jobs ORDER BY updated_at DESC LIMIT $1",
		limit,
	)
	return jobs, err
}

// CountByStatus — powers the stats endpoint with real historical data
func (s *PostgresStore) CountByStatus() (map[string]int, error) {
	rows, err := s.db.Queryx("SELECT status, COUNT(*) FROM jobs GROUP BY status")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[string]int)
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[status] = count
	}
	return counts, nil
}

// Purge completely wipes all job data from the database
func (s *PostgresStore) Purge() error {
	_, err := s.db.Exec("TRUNCATE TABLE jobs")
	return err
}