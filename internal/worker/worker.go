package worker

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

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
	p.persist(job)
	
	var duration time.Duration
	var simulatedErr bool

	switch job.Type {
	case "email_dispatch":
		var payload EmailPayload
		if err := json.Unmarshal([]byte(job.Payload), &payload); err == nil {
			log.Printf("worker %d: [EMAIL] Sending %s to %s", workerID, payload.TemplateID, payload.To)
		}
		duration = time.Duration(100+rand.Intn(200)) * time.Millisecond
		simulatedErr = rand.Intn(100) < 5 // 5% failure rate for emails

	case "video_encoding":
		var payload VideoPayload
		if err := json.Unmarshal([]byte(job.Payload), &payload); err == nil {
			log.Printf("worker %d: [VIDEO] Encoding %s at %s", workerID, payload.VideoID, payload.Resolution)
		}
		duration = time.Duration(1000+rand.Intn(2000)) * time.Millisecond
		simulatedErr = rand.Intn(100) < 15 // 15% failure rate for video encoding

	case "data_ingestion":
		var payload DataPayload
		if err := json.Unmarshal([]byte(job.Payload), &payload); err == nil {
			log.Printf("worker %d: [DATA] Ingesting %s from bucket %s", workerID, payload.FilePath, payload.S3Bucket)
		}
		duration = time.Duration(500+rand.Intn(1000)) * time.Millisecond
		simulatedErr = rand.Intn(100) < 10 // 10% failure rate for data ingestion

	default:
		log.Printf("worker %d: processing unknown job %s (type=%s)", workerID, job.ID, job.Type)
		duration = time.Duration(200+rand.Intn(800)) * time.Millisecond
		simulatedErr = rand.Intn(100) < 20
	}

	time.Sleep(duration)

	if simulatedErr {
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
