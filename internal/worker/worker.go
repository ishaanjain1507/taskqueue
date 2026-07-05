package worker

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/ishaanjain1507/taskqueue/internal/db"
	"github.com/ishaanjain1507/taskqueue/internal/models"
)

type Pool struct {
	queue      models.Queue
	store      models.Store
	numWorkers int
}

func NewPool(q models.Queue, store models.Store, numWorkers int) *Pool {
	return &Pool{
		queue:      q,
		store:      store,
		numWorkers: numWorkers,
	}
}

func (p *Pool) Start(ctx context.Context) {
	for i := 1; i <= p.numWorkers; i++ {
		go p.runWorker(ctx, i)
	}
	log.Printf("started %d workers", p.numWorkers)
}

func (p *Pool) runWorker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d shutting down", id)
			return
		default:
			job, err := p.queue.Dequeue(ctx)
			if err != nil {
				log.Printf("worker %d: dequeue error: %v", id, err)
				continue
			}
			if job == nil {
				continue
			}

			p.processJob(ctx, id, job)
		}
	}
}

func (p *Pool) processJob(ctx context.Context, workerID int, job *models.Job) {
	job.Status = models.StatusProcessing
	p.persist(job)
	log.Printf("worker %d: processing job %s (type=%s)", workerID, job.ID, job.Type)

	duration := time.Duration(200+rand.Intn(800)) * time.Millisecond
	time.Sleep(duration)
	failed := rand.Intn(100) < 20

	if failed {
		p.handleFailure(ctx, workerID, job)
		return
	}

	job.Status = models.StatusSuccess
	job.UpdatedAt = time.Now()
	p.persist(job)
	log.Printf("worker %d: job %s succeeded (took %v)", workerID, job.ID, duration)
}

func (p *Pool) handleFailure(ctx context.Context, workerID int, job *models.Job) {
	job.Retries++

	if job.Retries >= job.MaxRetries {
		job.Status = models.StatusDead
		job.Error = "max retries exceeded"
		job.UpdatedAt = time.Now()
		p.persist(job)
		log.Printf("worker %d: job %s exhausted retries, sending to DLQ", workerID, job.ID)

		if err := p.queue.SendToDead(ctx, job); err != nil {
			log.Printf("worker %d: failed to send job %s to DLQ: %v", workerID, job.ID, err)
		}
		return
	}

	backoff := time.Duration(1<<job.Retries) * time.Second
	job.Status = models.StatusPending
	job.Error = "retrying after failure"
	job.UpdatedAt = time.Now()
	p.persist(job)

	log.Printf("worker %d: job %s failed (attempt %d/%d), retrying in %v",
		workerID, job.ID, job.Retries, job.MaxRetries, backoff)

	time.Sleep(backoff)

	if err := p.queue.Enqueue(ctx, job); err != nil {
		log.Printf("worker %d: failed to re-enqueue job %s: %v", workerID, job.ID, err)
	}
}

// persist writes the current job state to Postgres.
// We log errors but don't crash the worker — Postgres being briefly
// unavailable shouldn't stop job processing, just historical tracking.
func (p *Pool) persist(job *models.Job) {
	if err := p.store.UpsertJob(job); err != nil {
		log.Printf("failed to persist job %s: %v", job.ID, err)
	}
}
