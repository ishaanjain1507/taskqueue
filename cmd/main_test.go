package main

import "testing"

func TestServiceURL(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		fallback string
		want     string
	}{
		{name: "render hostname", value: "taskqueue-grafana.onrender.com", want: "https://taskqueue-grafana.onrender.com"},
		{name: "absolute URL", value: "https://metrics.example.com/", want: "https://metrics.example.com"},
		{name: "local fallback", fallback: "http://localhost:3000", want: "http://localhost:3000"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := serviceURL(test.value, test.fallback); got != test.want {
				t.Fatalf("serviceURL() = %q, want %q", got, test.want)
			}
		})
	}
}
