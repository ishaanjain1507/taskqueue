import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    constant_request_rate: {
      executor: 'constant-arrival-rate',
      rate: 1000,           // 1000 requests per second
      timeUnit: '1s',
      duration: '10s',      // Run for 10 seconds (10,000 requests total)
      preAllocatedVUs: 100, // Pre-allocate 100 Virtual Users
      maxVUs: 500,          // Maximum allowed VUs
    },
  },
};

export default function () {
  const url = 'http://localhost:8080/jobs';
  const payload = JSON.stringify({
    type: 'video_encoding',
    payload: '{"url": "https://example.com/video.mp4"}',
    max_retries: 3,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);
  
  check(res, {
    'is status 202': (r) => r.status === 202,
  });
}
