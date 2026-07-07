package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ishaanjain1507/taskqueue/internal/models"
)

const (
	MainQueue = "taskqueue:main" // primary job queue
	DeadQueue = "taskqueue:dead" // dead letter queue
)

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue(redisURL string) (*RedisQueue, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}

	// Each worker holds a connection during BRPop (blocking dequeue).
	// Pool must be >= max workers + headroom for enqueue/stats calls.
	opts.PoolSize = 200
	opts.MinIdleConns = 10

	client := redis.NewClient(opts)

	// verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("could not connect to redis: %w", err)
	}

	return &RedisQueue{client: client}, nil
}

// Enqueue serialises a job to JSON and pushes it to the LEFT of the list
// LPUSH is O(1) — constant time regardless of queue size
func (q *RedisQueue) Enqueue(ctx context.Context, job *models.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to serialise job: %w", err)
	}

	if err := q.client.LPush(ctx, MainQueue, data).Err(); err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}

	return nil
}

// Dequeue blocks until a job is available, then pops from the RIGHT
// BRPOP = Blocking Right POP — workers sleep here until work arrives
// This is why CPU is 0% when idle — no polling loop
func (q *RedisQueue) Dequeue(ctx context.Context) (*models.Job, error) {
	// blocks for up to 5 seconds, then returns nil so worker can check for shutdown
	result, err := q.client.BRPop(ctx, 5*time.Second, MainQueue).Result()
	if err == redis.Nil {
		return nil, nil // timeout, no job available — not an error
	}
	if err != nil {
		return nil, fmt.Errorf("failed to dequeue job: %w", err)
	}

	var job models.Job
	// result[0] is the queue name, result[1] is the actual value
	if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
		return nil, fmt.Errorf("failed to deserialise job: %w", err)
	}

	return &job, nil
}

// SendToDead moves a permanently failed job to the Dead Letter Queue
// Jobs here can be inspected, retried manually, or alerted on
func (q *RedisQueue) SendToDead(ctx context.Context, job *models.Job) error {
	job.Status = models.StatusDead
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to serialise dead job: %w", err)
	}

	if err := q.client.LPush(ctx, DeadQueue, data).Err(); err != nil {
		return fmt.Errorf("failed to send job to DLQ: %w", err)
	}

	return nil
}

// QueueLength returns current depth of main and dead queues
func (q *RedisQueue) QueueLength(ctx context.Context) (main int64, dead int64, err error) {
	main, err = q.client.LLen(ctx, MainQueue).Result()
	if err != nil {
		return 0, 0, err
	}
	dead, err = q.client.LLen(ctx, DeadQueue).Result()
	return main, dead, err
}

// Purge completely wipes all job data from the Redis queue
func (q *RedisQueue) Purge(ctx context.Context) error {
	return q.client.FlushDB(ctx).Err()
}
