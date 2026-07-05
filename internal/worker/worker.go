package worker

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/ishaanjain1507/taskqueue/internal/models"
	"github.com/ishaanjain1507/taskqueue/internal/queue"
)

type Pool struct {
	queue      *queue.RedisQueue
	numWorkers int
}

func NewPool(q *queue.RedisQueue, numWorkers int) *Pool {
	return &Pool{
		queue:      q,
		numWorkers: numWorkers,
	}
}

// Start launches numWorkers goroutines that each independently
// pull jobs from Redis and process them. ctx lets us shut down cleanly.
func (p *Pool) Start(ctx context.Context) {
	for i := 1; i <= p.numWorkers; i++ {
		go p.runWorker(ctx, i)
	}
	log.Printf("started %d workers", p.numWorkers)
}

// runWorker is the loop each goroutine executes independently.
// Each worker has its own identity (id) for logging, but they all
// share the same Redis connection safely — Redis handles the atomicity.
func (p *Pool) runWorker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			// context was cancelled — shut down gracefully
			log.Printf("worker %d shutting down", id)
			return
		default:
			job, err := p.queue.Dequeue(ctx)
			if err != nil {
				log.Printf("worker %d: dequeue error: %v", id, err)
				continue
			}
			if job == nil {
				// BRPOP timed out with no job available — loop back and try again
				continue
			}

			p.processJob(ctx, id, job)
		}
	}
}

// processJob simulates doing actual work, then handles success/failure/retry logic
func (p *Pool) processJob(ctx context.Context, workerID int, job *models.Job) {
	job.Status = models.StatusProcessing
	log.Printf("worker %d: processing job %s (type=%s)", workerID, job.ID, job.Type)

	// Simulate real work — random duration + random 20% failure rate
	// In a real system this is where you'd call the actual task logic
	duration := time.Duration(200+rand.Intn(800)) * time.Millisecond
	time.Sleep(duration)
	failed := rand.Intn(100) < 20

	if failed {
		p.handleFailure(ctx, workerID, job)
		return
	}

	job.Status = models.StatusSuccess
	job.UpdatedAt = time.Now()
	log.Printf("worker %d: job %s succeeded (took %v)", workerID, job.ID, duration)
}

// handleFailure implements retry with exponential backoff.
// 1st retry waits 1s, 2nd waits 2s, 3rd waits 4s — backing off each time
// so we don't hammer a struggling downstream system.

func (p *Pool) handleFailure(ctx context.Context, workerID int, job *models.Job) {
	job.Retries++

	if job.Retries >= job.MaxRetries {
		job.Status = models.StatusDead
		job.Error = "max retries exceeded"
		log.Printf("worker %d: job %s exhausted retries, sending to DLQ", workerID, job.ID)

		if err := p.queue.SendToDead(ctx, job); err != nil {
			log.Printf("worker %d: failed to send job %s to DLQ: %v", workerID, job.ID, err)
		}
		return
	}

	// exponential backoff: 2^retries seconds (1s, 2s, 4s...)
	backoff := time.Duration(1<<job.Retries) * time.Second
	log.Printf("worker %d: job %s failed (attempt %d/%d), retrying in %v",
		workerID, job.ID, job.Retries, job.MaxRetries, backoff)

	job.Status = models.StatusPending
	time.Sleep(backoff)

	if err := p.queue.Enqueue(ctx, job); err != nil {
		log.Printf("worker %d: failed to re-enqueue job %s: %v", workerID, job.ID, err)
	}
}