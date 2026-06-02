package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Mode              string
	KafkaBrokers      []string
	EventsTopic       string
	DLQTopic          string
	ConsumerGroup     string
	SchemaRegistryURL string
	CassandraHosts    []string
	CassandraKeyspace string
	HTTPAddr          string
	ReadConsistency   string
	WriteConsistency  string
	ProducerInterval  time.Duration
}

func Load() Config {
	return Config{
		Mode:              getenv("APP_MODE", "consumer"),
		KafkaBrokers:      split(getenv("KAFKA_BROKERS", "localhost:9092")),
		EventsTopic:       getenv("KAFKA_EVENTS_TOPIC", "warehouse-events"),
		DLQTopic:          getenv("KAFKA_DLQ_TOPIC", "warehouse-events-dlq"),
		ConsumerGroup:     getenv("KAFKA_CONSUMER_GROUP", "warehouse-state-consumer"),
		SchemaRegistryURL: getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		CassandraHosts:    split(getenv("CASSANDRA_HOSTS", "localhost")),
		CassandraKeyspace: getenv("CASSANDRA_KEYSPACE", "warehouse"),
		HTTPAddr:          getenv("HTTP_ADDR", ":8080"),
		ReadConsistency:   getenv("CASSANDRA_READ_CONSISTENCY", "QUORUM"),
		WriteConsistency:  getenv("CASSANDRA_WRITE_CONSISTENCY", "QUORUM"),
		ProducerInterval:  time.Duration(getint("PRODUCER_INTERVAL_SECONDS", 0)) * time.Second,
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getint(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func split(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
