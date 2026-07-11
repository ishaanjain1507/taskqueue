package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ishaanjain1507/taskqueue/internal/models"
)

const (
	totalJobs   = 10000
	concurrency = 100
	apiURL      = "http://localhost:8080/jobs"
)

func main() {
	fmt.Printf("Starting load test: %d jobs across %d concurrent workers\n", totalJobs, concurrency)

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        concurrency * 2,
			MaxIdleConnsPerHost: concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	var successCount atomic.Int32
	var failCount atomic.Int32

	start := time.Now()

	var wg sync.WaitGroup
	jobsPerWorker := totalJobs / concurrency

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < jobsPerWorker; j++ {
				payload := generateRandomJob()
				req, err := http.NewRequest(http.MethodPost, apiURL, bytes.NewBuffer(payload))
				if err != nil {
					failCount.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if err != nil {
					failCount.Add(1)
					continue
				}

				if resp.StatusCode == http.StatusAccepted {
					successCount.Add(1)
				} else {
					failCount.Add(1)
				}
				resp.Body.Close()
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("Load test completed in %v\n", duration)
	fmt.Printf("Successful API Submissions: %d\n", successCount.Load())
	fmt.Printf("Failed API Submissions: %d\n", failCount.Load())

	if duration.Seconds() > 0 {
		throughput := float64(successCount.Load()) / duration.Seconds()
		fmt.Printf("API Throughput: %.2f requests/sec\n", throughput)
	}
}

func generateRandomJob() []byte {
	jobTypes := []string{"email_dispatch", "video_encoding", "data_ingestion"}
	jobType := jobTypes[rand.Intn(len(jobTypes))]
	maxRetries := 3

	var payloadStr string
	switch jobType {
	case "email_dispatch":
		payloadStr = fmt.Sprintf(`{"to": "user%d@example.com", "template_id": "welcome"}`, rand.Intn(10000))
	case "video_encoding":
		payloadStr = fmt.Sprintf(`{"video_id": "vid_%d", "resolution": "1080p"}`, rand.Intn(10000))
	case "data_ingestion":
		payloadStr = fmt.Sprintf(`{"s3_bucket": "my-data-%d", "file_path": "/imports/data.csv"}`, rand.Intn(10))
	}

	jobReq := models.CreateJobRequest{
		Type:       jobType,
		Payload:    payloadStr,
		MaxRetries: &maxRetries,
	}

	data, _ := json.Marshal(jobReq)
	return data
}
