package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"smart-warehouse/internal/application/warehouse"
	"smart-warehouse/internal/application/wms"
	cassandrainfra "smart-warehouse/internal/infrastructure/cassandra"
	"smart-warehouse/internal/infrastructure/config"
	"smart-warehouse/internal/infrastructure/httpserver"
	kafkainfra "smart-warehouse/internal/infrastructure/kafka"
	"smart-warehouse/internal/infrastructure/logger"
	"smart-warehouse/internal/infrastructure/metrics"
	"smart-warehouse/internal/infrastructure/schemaregistry"
)

func main() {
	log := logger.New()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	cfg := config.Load()
	log.Info("config loaded", "mode", cfg.Mode, "kafka_brokers", cfg.KafkaBrokers, "events_topic", cfg.EventsTopic, "consumer_group", cfg.ConsumerGroup, "cassandra_hosts", cfg.CassandraHosts, "read_cl", cfg.ReadConsistency, "write_cl", cfg.WriteConsistency)

	registryClient := schemaregistry.New(cfg.SchemaRegistryURL)
	codec, err := kafkainfra.NewAvroCodec(registryClient)
	if err != nil {
		return fmt.Errorf("create avro codec: %w", err)
	}
	if err := waitForSchemas(ctx, codec); err != nil {
		return fmt.Errorf("register avro schemas: %w", err)
	}

	switch cfg.Mode {
	case "consumer":
		return runConsumer(ctx, log, cfg, codec)
	case "producer":
		return runProducer(ctx, log, cfg, codec)
	default:
		return fmt.Errorf("unsupported APP_MODE %q", cfg.Mode)
	}
}

func runConsumer(ctx context.Context, log *slog.Logger, cfg config.Config, codec *kafkainfra.AvroCodec) error {
	repository, err := cassandrainfra.NewRepository(cfg.CassandraHosts, cfg.CassandraKeyspace, cfg.ReadConsistency, cfg.WriteConsistency)
	if err != nil {
		return fmt.Errorf("create cassandra repository: %w", err)
	}
	defer repository.Close()

	registry := metrics.New()
	processor := warehouse.NewProcessor(log, repository, registry)
	dlq := kafkainfra.NewDLQProducer(cfg.KafkaBrokers, cfg.DLQTopic)
	defer dlq.Close()
	consumer := kafkainfra.NewConsumer(log, cfg.KafkaBrokers, cfg.EventsTopic, cfg.ConsumerGroup, codec, processor, dlq, registry)

	server := httpserver.New(cfg.HTTPAddr, log, registry, repository, consumer, nil)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := server.Run(); err != nil {
			log.Error("http server stopped", "error", err)
		}
	}()
	log.Info("consumer started", "http_addr", cfg.HTTPAddr)
	return consumer.Run(ctx)
}

func runProducer(ctx context.Context, log *slog.Logger, cfg config.Config, codec *kafkainfra.AvroCodec) error {
	registry := metrics.New()
	producer := kafkainfra.NewProducer(cfg.KafkaBrokers, cfg.EventsTopic, codec)
	defer producer.Close()
	server := httpserver.New(cfg.HTTPAddr, log, registry, nil, nil, producer)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	generator := wms.NewGenerator(log, producer, cfg.ProducerInterval)
	go func() {
		if err := generator.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("event generator stopped", "error", err)
		}
	}()
	log.Info("producer started", "http_addr", cfg.HTTPAddr)
	return server.Run()
}

func waitForSchemas(ctx context.Context, codec *kafkainfra.AvroCodec) error {
	var lastErr error
	for attempt := 0; attempt < 60; attempt++ {
		if err := codec.RegisterSchemas(ctx); err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
				continue
			}
		}
		return nil
	}
	return lastErr
}
