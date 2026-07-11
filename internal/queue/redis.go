package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/ishaanjain1507/taskqueue/internal/models"
	"github.com/redis/go-redis/v9"
)

const (
	MainQueue     = "taskqueue:stream:main"
	DeadQueue     = "taskqueue:stream:dead"
	legacyMain    = "taskqueue:main"
	legacyDead    = "taskqueue:dead"
	consumerGroup = "taskqueue:workers"
	jobField      = "job"
	claimAfter    = 30 * time.Second
)

type RedisQueue struct {
	client   *redis.Client
	consumer string
}

func NewRedisQueue(redisURL string) (*RedisQueue, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}
	opts.PoolSize = 200
	opts.MinIdleConns = 10

	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("could not connect to redis: %w", err)
	}

	q := &RedisQueue{
		client:   client,
		consumer: fmt.Sprintf("%s-%s", hostname(), uuid.NewString()),
	}
	if err := q.migrateLegacyQueues(ctx); err != nil {
		return nil, err
	}
	if err := q.ensureConsumerGroup(ctx); err != nil {
		return nil, err
	}
	return q, nil
}

// migrateLegacyQueues preserves jobs created by releases that used Redis
// lists. Each list is copied oldest-first into its replacement stream and
// deleted in the same Redis transaction.
func (q *RedisQueue) migrateLegacyQueues(ctx context.Context) error {
	for _, migration := range []struct{ source, destination string }{
		{legacyMain, MainQueue},
		{legacyDead, DeadQueue},
	} {
		keyType, err := q.client.Type(ctx, migration.source).Result()
		if err != nil {
			return fmt.Errorf("failed to inspect legacy queue: %w", err)
		}
		if keyType == "none" {
			continue
		}
		if keyType != "list" {
			return fmt.Errorf("legacy queue %s has unexpected Redis type %s", migration.source, keyType)
		}
		entries, err := q.client.LRange(ctx, migration.source, 0, -1).Result()
		if err != nil {
			return fmt.Errorf("failed to read legacy queue: %w", err)
		}
		_, err = q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for i := len(entries) - 1; i >= 0; i-- {
				pipe.XAdd(ctx, &redis.XAddArgs{
					Stream: migration.destination,
					Values: map[string]interface{}{jobField: entries[i]},
				})
			}
			pipe.Del(ctx, migration.source)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to migrate legacy queue: %w", err)
		}
	}
	return nil
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "worker"
	}
	return name
}

func (q *RedisQueue) ensureConsumerGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, MainQueue, consumerGroup, "0").Err()
	if err != nil && !redis.HasErrorPrefix(err, "BUSYGROUP") {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}
	return nil
}

func (q *RedisQueue) Enqueue(ctx context.Context, job *models.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to serialise job: %w", err)
	}
	if err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: MainQueue,
		Values: map[string]interface{}{jobField: data},
	}).Err(); err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}
	return nil
}

// Dequeue reserves a message. It remains pending until Ack, Requeue, or
// SendToDead succeeds, allowing another worker to reclaim abandoned work.
func (q *RedisQueue) Dequeue(ctx context.Context) (*models.Delivery, error) {
	claimed, _, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   MainQueue,
		Group:    consumerGroup,
		Consumer: q.consumer,
		MinIdle:  claimAfter,
		Start:    "0-0",
		Count:    1,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to reclaim abandoned job: %w", err)
	}
	if len(claimed) > 0 {
		return decodeDelivery(claimed[0])
	}

	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    consumerGroup,
		Consumer: q.consumer,
		Streams:  []string{MainQueue, ">"},
		Count:    1,
		Block:    5 * time.Second,
	}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to dequeue job: %w", err)
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil, nil
	}
	return decodeDelivery(streams[0].Messages[0])
}

func decodeDelivery(message redis.XMessage) (*models.Delivery, error) {
	raw, ok := message.Values[jobField]
	if !ok {
		return nil, fmt.Errorf("message %s has no job payload", message.ID)
	}
	var data []byte
	switch value := raw.(type) {
	case string:
		data = []byte(value)
	case []byte:
		data = value
	default:
		return nil, fmt.Errorf("message %s has invalid job payload", message.ID)
	}
	var job models.Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("failed to deserialise job: %w", err)
	}
	return &models.Delivery{Job: &job, Receipt: message.ID}, nil
}

func (q *RedisQueue) Ack(ctx context.Context, receipt string) error {
	_, err := q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.XAck(ctx, MainQueue, consumerGroup, receipt)
		pipe.XDel(ctx, MainQueue, receipt)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to acknowledge job: %w", err)
	}
	return nil
}

func (q *RedisQueue) Requeue(ctx context.Context, receipt string, job *models.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to serialise retry: %w", err)
	}
	_, err = q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: MainQueue, Values: map[string]interface{}{jobField: data}})
		pipe.XAck(ctx, MainQueue, consumerGroup, receipt)
		pipe.XDel(ctx, MainQueue, receipt)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to requeue job: %w", err)
	}
	return nil
}

func (q *RedisQueue) SendToDead(ctx context.Context, receipt string, job *models.Job) error {
	job.Status = models.StatusDead
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to serialise dead job: %w", err)
	}
	_, err = q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: DeadQueue, Values: map[string]interface{}{jobField: data}})
		pipe.XAck(ctx, MainQueue, consumerGroup, receipt)
		pipe.XDel(ctx, MainQueue, receipt)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to send job to DLQ: %w", err)
	}
	return nil
}

func (q *RedisQueue) QueueLength(ctx context.Context) (main int64, dead int64, err error) {
	main, err = q.client.XLen(ctx, MainQueue).Result()
	if err != nil {
		return 0, 0, err
	}
	dead, err = q.client.XLen(ctx, DeadQueue).Result()
	return main, dead, err
}

// Purge removes only taskqueue-owned keys, never the entire Redis database.
func (q *RedisQueue) Purge(ctx context.Context) error {
	if err := q.client.Del(ctx, MainQueue, DeadQueue, legacyMain, legacyDead).Err(); err != nil {
		return err
	}
	return q.ensureConsumerGroup(ctx)
}
