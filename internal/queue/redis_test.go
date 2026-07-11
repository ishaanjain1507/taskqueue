package queue

import (
	"encoding/json"
	"testing"

	"github.com/ishaanjain1507/taskqueue/internal/models"
	"github.com/redis/go-redis/v9"
)

func TestDecodeDelivery(t *testing.T) {
	want := &models.Job{ID: "job-1", Type: "email_dispatch", Status: models.StatusPending}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	delivery, err := decodeDelivery(redis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{jobField: string(data)},
	})
	if err != nil {
		t.Fatalf("decodeDelivery returned an error: %v", err)
	}
	if delivery.Receipt != "1-0" || delivery.Job.ID != want.ID {
		t.Fatalf("unexpected delivery: %#v", delivery)
	}
}

func TestDecodeDeliveryRejectsMissingPayload(t *testing.T) {
	if _, err := decodeDelivery(redis.XMessage{ID: "1-0"}); err == nil {
		t.Fatal("expected an error for a message without a job payload")
	}
}
