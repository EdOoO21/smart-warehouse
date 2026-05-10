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
	writeErrors     uint64
	durationBuckets map[float64]uint64
	durationSum     float64
	durationCount   uint64
	lag             map[int]int64
}

func New() *Registry {
	return &Registry{
		processed: map[string]uint64{},
		failed:    map[string]uint64{},
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

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.mu.RLock()
		defer r.mu.RUnlock()

		var b strings.Builder
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
