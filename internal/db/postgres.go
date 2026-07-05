package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // driver registers itself, we never call it directly
	"github.com/ishaanjain1507/taskqueue/internal/models"
)

type PostgresStore struct {
	db *sqlx.DB
}

func NewPostgresStore(connStr string) (*PostgresStore, error) {
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	store := &PostgresStore{db: db}
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
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	);
	CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
	CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
	`
	_, err := s.db.Exec(schema)
	return err
}

// UpsertJob inserts a new job or updates it if the ID already exists.
// This is called at every state transition: created, processing, success, failed, dead.
func (s *PostgresStore) UpsertJob(job *models.Job) error {
	query := `
	INSERT INTO jobs (id, type, payload, status, retries, max_retries, error, created_at, updated_at)
	VALUES (:id, :type, :payload, :status, :retries, :max_retries, :error, :created_at, :updated_at)
	ON CONFLICT (id) DO UPDATE SET
		status = EXCLUDED.status,
		retries = EXCLUDED.retries,
		error = EXCLUDED.error,
		updated_at = EXCLUDED.updated_at
	`
	_, err := s.db.NamedExec(query, job)
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