package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

type Registry struct {
	mu              sync.RWMutex
	processed       map[string]uint64
	failed          map[string]uint64
	httpRequests    map[httpRequestKey]uint64
	httpErrors      map[httpErrorKey]uint64
	httpDurations   map[httpDurationKey]*histogram
	writeErrors     uint64
	durationBuckets map[float64]uint64
	durationSum     float64
	durationCount   uint64
	lag             map[int]int64
}

type httpRequestKey struct {
	Method   string
	Endpoint string
	Status   string
}

type httpErrorKey struct {
	Method    string
	Endpoint  string
	ErrorType string
}

type httpDurationKey struct {
	Method   string
	Endpoint string
}

type histogram struct {
	Buckets map[float64]uint64
	Sum     float64
	Count   uint64
}

func New() *Registry {
	return &Registry{
		processed:     map[string]uint64{},
		failed:        map[string]uint64{},
		httpRequests:  map[httpRequestKey]uint64{},
		httpErrors:    map[httpErrorKey]uint64{},
		httpDurations: map[httpDurationKey]*histogram{},
		durationBuckets: map[float64]uint64{
			0.005: 0, 0.01: 0, 0.025: 0, 0.05: 0, 0.1: 0, 0.25: 0, 0.5: 0, 1: 0, 2.5: 0, 5: 0, 10: 0,
		},
		lag: map[int]int64{},
	}
}

func (r *Registry) IncProcessed(eventType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.processed[eventType]++
}

func (r *Registry) IncFailed(errorCode string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed[errorCode]++
}

func (r *Registry) ObserveDuration(seconds float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for bucket := range r.durationBuckets {
		if seconds <= bucket {
			r.durationBuckets[bucket]++
		}
	}
	r.durationSum += seconds
	r.durationCount++
}

func (r *Registry) IncCassandraWriteError() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeErrors++
}

func (r *Registry) SetLag(partition int, lag int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lag < 0 {
		lag = 0
	}
	r.lag[partition] = lag
}

func (r *Registry) ObserveHTTPRequest(method, endpoint string, status int, seconds float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	statusText := fmt.Sprint(status)
	r.httpRequests[httpRequestKey{Method: method, Endpoint: endpoint, Status: statusText}]++
	if status >= 500 {
		r.httpErrors[httpErrorKey{Method: method, Endpoint: endpoint, ErrorType: "server_error"}]++
	} else if status >= 400 {
		r.httpErrors[httpErrorKey{Method: method, Endpoint: endpoint, ErrorType: "client_error"}]++
	}
	key := httpDurationKey{Method: method, Endpoint: endpoint}
	h, ok := r.httpDurations[key]
	if !ok {
		h = newHistogram()
		r.httpDurations[key] = h
	}
	for bucket := range h.Buckets {
		if seconds <= bucket {
			h.Buckets[bucket]++
		}
	}
	h.Sum += seconds
	h.Count++
}

func newHistogram() *histogram {
	return &histogram{Buckets: map[float64]uint64{
		0.005: 0, 0.01: 0, 0.025: 0, 0.05: 0, 0.1: 0, 0.25: 0, 0.5: 0, 1: 0, 2.5: 0, 5: 0, 10: 0,
	}}
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.mu.RLock()
		defer r.mu.RUnlock()

		var b strings.Builder
		b.WriteString("# TYPE http_requests_total counter\n")
		requestKeys := make([]httpRequestKey, 0, len(r.httpRequests))
		for key := range r.httpRequests {
			requestKeys = append(requestKeys, key)
		}
		sort.Slice(requestKeys, func(i, j int) bool {
			if requestKeys[i].Endpoint == requestKeys[j].Endpoint {
				if requestKeys[i].Method == requestKeys[j].Method {
					return requestKeys[i].Status < requestKeys[j].Status
				}
				return requestKeys[i].Method < requestKeys[j].Method
			}
			return requestKeys[i].Endpoint < requestKeys[j].Endpoint
		})
		for _, key := range requestKeys {
			fmt.Fprintf(&b, "http_requests_total{method=%q,endpoint=%q,status=%q} %d\n", key.Method, key.Endpoint, key.Status, r.httpRequests[key])
		}
		b.WriteString("# TYPE http_request_errors_total counter\n")
		errorKeys := make([]httpErrorKey, 0, len(r.httpErrors))
		for key := range r.httpErrors {
			errorKeys = append(errorKeys, key)
		}
		sort.Slice(errorKeys, func(i, j int) bool {
			if errorKeys[i].Endpoint == errorKeys[j].Endpoint {
				if errorKeys[i].Method == errorKeys[j].Method {
					return errorKeys[i].ErrorType < errorKeys[j].ErrorType
				}
				return errorKeys[i].Method < errorKeys[j].Method
			}
			return errorKeys[i].Endpoint < errorKeys[j].Endpoint
		})
		for _, key := range errorKeys {
			fmt.Fprintf(&b, "http_request_errors_total{method=%q,endpoint=%q,error_type=%q} %d\n", key.Method, key.Endpoint, key.ErrorType, r.httpErrors[key])
		}
		b.WriteString("# TYPE http_request_duration_seconds histogram\n")
		durationKeys := make([]httpDurationKey, 0, len(r.httpDurations))
		for key := range r.httpDurations {
			durationKeys = append(durationKeys, key)
		}
		sort.Slice(durationKeys, func(i, j int) bool {
			if durationKeys[i].Endpoint == durationKeys[j].Endpoint {
				return durationKeys[i].Method < durationKeys[j].Method
			}
			return durationKeys[i].Endpoint < durationKeys[j].Endpoint
		})
		for _, key := range durationKeys {
			h := r.httpDurations[key]
			buckets := make([]float64, 0, len(h.Buckets))
			for bucket := range h.Buckets {
				buckets = append(buckets, bucket)
			}
			sort.Float64s(buckets)
			for _, bucket := range buckets {
				fmt.Fprintf(&b, "http_request_duration_seconds_bucket{method=%q,endpoint=%q,le=%q} %d\n", key.Method, key.Endpoint, fmt.Sprintf("%g", bucket), h.Buckets[bucket])
			}
			fmt.Fprintf(&b, "http_request_duration_seconds_bucket{method=%q,endpoint=%q,le=\"+Inf\"} %d\n", key.Method, key.Endpoint, h.Count)
			fmt.Fprintf(&b, "http_request_duration_seconds_sum{method=%q,endpoint=%q} %g\n", key.Method, key.Endpoint, h.Sum)
			fmt.Fprintf(&b, "http_request_duration_seconds_count{method=%q,endpoint=%q} %d\n", key.Method, key.Endpoint, h.Count)
		}
		b.WriteString("# TYPE events_processed_total counter\n")
		eventTypes := make([]string, 0, len(r.processed))
		for eventType := range r.processed {
			eventTypes = append(eventTypes, eventType)
		}
		sort.Strings(eventTypes)
		for _, eventType := range eventTypes {
			fmt.Fprintf(&b, "events_processed_total{event_type=%q} %d\n", eventType, r.processed[eventType])
		}
		b.WriteString("# TYPE events_failed_total counter\n")
		errorCodes := make([]string, 0, len(r.failed))
		for errorCode := range r.failed {
			errorCodes = append(errorCodes, errorCode)
		}
		sort.Strings(errorCodes)
		for _, errorCode := range errorCodes {
			fmt.Fprintf(&b, "events_failed_total{error_code=%q} %d\n", errorCode, r.failed[errorCode])
		}
		b.WriteString("# TYPE event_processing_duration_seconds histogram\n")
		buckets := make([]float64, 0, len(r.durationBuckets))
		for bucket := range r.durationBuckets {
			buckets = append(buckets, bucket)
		}
		sort.Float64s(buckets)
		for _, bucket := range buckets {
			fmt.Fprintf(&b, "event_processing_duration_seconds_bucket{le=%q} %d\n", fmt.Sprintf("%g", bucket), r.durationBuckets[bucket])
		}
		fmt.Fprintf(&b, "event_processing_duration_seconds_bucket{le=\"+Inf\"} %d\n", r.durationCount)
		fmt.Fprintf(&b, "event_processing_duration_seconds_sum %g\n", r.durationSum)
		fmt.Fprintf(&b, "event_processing_duration_seconds_count %d\n", r.durationCount)
		b.WriteString("# TYPE cassandra_write_errors_total counter\n")
		fmt.Fprintf(&b, "cassandra_write_errors_total %d\n", r.writeErrors)
		b.WriteString("# TYPE consumer_lag gauge\n")
		partitions := make([]int, 0, len(r.lag))
		for partition := range r.lag {
			partitions = append(partitions, partition)
		}
		sort.Ints(partitions)
		for _, partition := range partitions {
			fmt.Fprintf(&b, "consumer_lag{partition=%q} %d\n", fmt.Sprint(partition), r.lag[partition])
		}
		_, _ = w.Write([]byte(b.String()))
	})
}
