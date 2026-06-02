package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"

	"smart-warehouse/internal/application/warehouse"
	"smart-warehouse/internal/domain"
)

type Consumer struct {
	reader    *kafka.Reader
	brokers   []string
	topic     string
	processor *warehouse.Processor
	codec     *AvroCodec
	dlq       warehouse.DLQPublisher
	metrics   warehouse.Metrics
	logger    *slog.Logger
}

func NewConsumer(logger *slog.Logger, brokers []string, topic, groupID string, codec *AvroCodec, processor *warehouse.Processor, dlq warehouse.DLQPublisher, metrics warehouse.Metrics) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	return &Consumer{reader: reader, brokers: brokers, topic: topic, processor: processor, codec: codec, dlq: dlq, metrics: metrics, logger: logger}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.reader.Close()
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return ctx.Err()
			}
			return fmt.Errorf("fetch kafka message: %w", err)
		}

		event, err := c.codec.Decode(ctx, msg.Value)
		if err == nil {
			err = c.processor.Process(ctx, event)
		}

		switch {
		case err == nil:
			c.logger.Info("warehouse event processed", "event_id", event.EventID, "event_type", event.EventType, "partition", msg.Partition, "offset", msg.Offset)
		case warehouse.IsSkippable(err):
			c.logger.Info("warehouse event skipped", "event_id", event.EventID, "event_type", event.EventType, "partition", msg.Partition, "offset", msg.Offset, "reason", err.Error())
		default:
			code := errorCode(err)
			c.metrics.IncFailed(code)
			c.logger.Error("warehouse event failed, sending to dlq", "partition", msg.Partition, "offset", msg.Offset, "error", err)
			if dlqErr := c.dlq.Publish(ctx, msg.Value, err.Error(), code, msg.Partition, msg.Offset); dlqErr != nil {
				c.logger.Error("failed to publish dlq event", "partition", msg.Partition, "offset", msg.Offset, "error", dlqErr)
				continue
			}
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit kafka offset partition=%d offset=%d: %w", msg.Partition, msg.Offset, err)
		}
		c.updateLag(ctx, msg.Partition)
	}
}

func (c *Consumer) Ping(ctx context.Context) error {
	conn, err := kafka.DialLeader(ctx, "tcp", c.brokers[0], c.topic, 0)
	if err != nil {
		return err
	}
	return conn.Close()
}

func (c *Consumer) updateLag(ctx context.Context, partition int) {
	conn, err := kafka.DialLeader(ctx, "tcp", c.brokers[0], c.topic, partition)
	if err != nil {
		return
	}
	defer conn.Close()
	latest, err := conn.ReadLastOffset()
	if err != nil {
		return
	}
	stats := c.reader.Stats()
	c.metrics.SetLag(partition, latest-stats.Offset)
}

func errorCode(err error) string {
	if errors.Is(err, domain.ErrStaleEvent) {
		return "STALE_EVENT"
	}
	return "VALIDATION_OR_PROCESSING_ERROR"
}
