package metrics

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// PrometheusMiddleware intercepts HTTP requests to record metrics
func PrometheusMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		
		// Process request
		c.Next()
		
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())
		
		// Ignore /metrics and static files to reduce noise
		if c.Request.URL.Path == "/metrics" || c.Request.URL.Path == "/health" {
			return
		}

		HttpRequestsTotal.WithLabelValues(c.Request.Method, c.FullPath(), status).Inc()
		HttpRequestDuration.WithLabelValues(c.Request.Method, c.FullPath()).Observe(duration)
	}
}
