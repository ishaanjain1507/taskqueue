package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/ishaanjain1507/taskqueue/internal/metrics"
	"github.com/ishaanjain1507/taskqueue/internal/models"
)

type Pool struct {
	queue      models.Queue
	store      models.Store
	
	mu         sync.Mutex
	numWorkers int
	workers    map[int]context.CancelFunc
	nextID     int
	parentCtx  context.Context
}

func NewPool(q models.Queue, store models.Store, initialWorkers int) *Pool {
	return &Pool{
		queue:      q,
		store:      store,
		numWorkers: initialWorkers,
		workers:    make(map[int]context.CancelFunc),
		nextID:     1,
	}
}

func (p *Pool) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.parentCtx = ctx
	
	for i := 0; i < p.numWorkers; i++ {
		p.startWorker()
	}
	log.Printf("Pool started with %d workers", p.numWorkers)
}

// startWorker spins up a single worker. Must be called with lock held.
func (p *Pool) startWorker() {
	id := p.nextID
	p.nextID++
	
	wCtx, cancel := context.WithCancel(p.parentCtx)
	p.workers[id] = cancel
	
	go func(workerID int) {
		p.runWorker(wCtx, workerID)
		
		// Cleanup when goroutine exits
		p.mu.Lock()
		delete(p.workers, workerID)
		p.mu.Unlock()
	}(id)
}

// Scale dynamically adjusts the number of running workers.
func (p *Pool) Scale(target int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	current := len(p.workers)
	if target > current {
		toStart := target - current
		for i := 0; i < toStart; i++ {
			p.startWorker()
		}
		log.Printf("Scaled UP to %d workers (started %d)", target, toStart)
	} else if target < current {
		toStop := current - target
		stopped := 0
		for id, cancel := range p.workers {
			if stopped >= toStop {
				break
			}
			cancel()
			delete(p.workers, id) // remove immediately so ActiveWorkers() is accurate
			stopped++
		}
		log.Printf("Scaled DOWN to %d workers (stopped %d)", target, toStop)
	}
	p.numWorkers = target
}

// ActiveWorkers returns the current number of running worker goroutines.
func (p *Pool) ActiveWorkers() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

func (p *Pool) runWorker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d shutting down gracefully", id)
			return
		default:
			job, err := p.queue.Dequeue(ctx)
			if err != nil {
				// Don't log or sleep on context cancellation — just exit
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					log.Printf("worker %d shutting down gracefully", id)
					return
				}
				log.Printf("worker %d: dequeue error: %v", id, err)
				time.Sleep(1 * time.Second)
				continue
			}
			if job == nil {
				continue
			}

			p.processJob(ctx, id, job)
		}
	}
}

type EmailPayload struct {
	To         string `json:"to"`
	TemplateID string `json:"template_id"`
}
type VideoPayload struct {
	VideoID    string `json:"video_id"`
	Resolution string `json:"resolution"`
}
type DataPayload struct {
	S3Bucket string `json:"s3_bucket"`
	FilePath string `json:"file_path"`
}

func (p *Pool) processJob(ctx context.Context, workerID int, job *models.Job) {
	job.Status = models.StatusProcessing
	job.WorkerID = workerID
	now := time.Now()
	job.StartedAt = &now
	p.persist(job)
	
	var duration time.Duration
	var simulatedErr bool

	switch job.Type {
	case "email_dispatch":
		var payload EmailPayload
		if err := json.Unmarshal([]byte(job.Payload), &payload); err == nil {
			log.Printf("worker %d: [EMAIL] Sending %s to %s", workerID, payload.TemplateID, payload.To)
		}
		duration = time.Duration(1000+rand.Intn(2000)) * time.Millisecond // 1-3 seconds
		simulatedErr = rand.Intn(100) < 5

	case "video_encoding":
		var payload VideoPayload
		if err := json.Unmarshal([]byte(job.Payload), &payload); err == nil {
			log.Printf("worker %d: [VIDEO] Encoding %s at %s", workerID, payload.VideoID, payload.Resolution)
		}
		duration = time.Duration(3000+rand.Intn(4000)) * time.Millisecond // 3-7 seconds
		simulatedErr = rand.Intn(100) < 15

	case "data_ingestion":
		var payload DataPayload
		if err := json.Unmarshal([]byte(job.Payload), &payload); err == nil {
			log.Printf("worker %d: [DATA] Ingesting %s from bucket %s", workerID, payload.FilePath, payload.S3Bucket)
		}
		duration = time.Duration(2000+rand.Intn(2000)) * time.Millisecond // 2-4 seconds
		simulatedErr = rand.Intn(100) < 10
		
	case "trigger_error":
		log.Printf("worker %d: [TEST] Intentionally failing job %s", workerID, job.ID)
		duration = time.Duration(500+rand.Intn(1000)) * time.Millisecond
		simulatedErr = true // 100% failure rate for DLQ testing

	default:
		log.Printf("worker %d: processing unknown job %s (type=%s)", workerID, job.ID, job.Type)
		duration = time.Duration(1000+rand.Intn(1000)) * time.Millisecond
		simulatedErr = rand.Intn(100) < 20
	}

	log.Printf("worker %d: processing job %s (type=%s, est. %v)", workerID, job.ID, job.Type, duration)

	// Simulate work — context-aware so shutdown doesn't block
	select {
	case <-time.After(duration):
		// work completed
	case <-ctx.Done():
		log.Printf("worker %d: interrupted during job %s", workerID, job.ID)
		return
	}

	if simulatedErr {
		metrics.JobsProcessedTotal.WithLabelValues("FAILED", job.Type).Inc()
		p.handleFailure(ctx, workerID, job)
		return
	}

	metrics.JobsProcessedTotal.WithLabelValues("SUCCESS", job.Type).Inc()
	metrics.JobProcessingDuration.WithLabelValues(job.Type).Observe(duration.Seconds())

	job.Status = models.StatusSuccess
	nowComplete := time.Now()
	job.CompletedAt = &nowComplete
	job.UpdatedAt = nowComplete
	p.persist(job)
	log.Printf("worker %d: job %s completed (took %v)", workerID, job.ID, duration)
}

func (p *Pool) handleFailure(ctx context.Context, workerID int, job *models.Job) {
	job.Retries++

	if job.Retries >= job.MaxRetries {
		job.Status = models.StatusDead
		job.Error = "max retries exceeded or intentional failure"
		nowComplete := time.Now()
		job.CompletedAt = &nowComplete
		job.UpdatedAt = nowComplete
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
	job.WorkerID = 0
	job.StartedAt = nil
	job.CompletedAt = nil
	job.UpdatedAt = time.Now()
	p.persist(job)

	log.Printf("worker %d: job %s failed (attempt %d/%d), retrying in %v",
		workerID, job.ID, job.Retries, job.MaxRetries, backoff)

	// Non-blocking backoff: respects context cancellation so scaling down works
	select {
	case <-time.After(backoff):
		// backoff complete, re-enqueue
	case <-ctx.Done():
		log.Printf("worker %d: cancelled during backoff for job %s", workerID, job.ID)
		return
	}

	if err := p.queue.Enqueue(ctx, job); err != nil {
		log.Printf("worker %d: failed to re-enqueue job %s: %v", workerID, job.ID, err)
	}
}

func (p *Pool) persist(job *models.Job) {
	if err := p.store.UpsertJob(job); err != nil {
		log.Printf("failed to persist job %s: %v", job.ID, err)
	}
}
