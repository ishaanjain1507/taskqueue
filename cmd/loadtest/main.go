package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
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

	jobReq := models.CreateJobRequest{
		Type:       "benchmark_job",
		Payload:    `{"test": "data"}`,
		MaxRetries: 3,
	}

	payload, err := json.Marshal(jobReq)
	if err != nil {
		log.Fatalf("failed to marshal request: %v", err)
	}

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
