package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"

	"smart-warehouse/internal/domain"
)

type Producer struct {
	writer *kafka.Writer
	codec  *AvroCodec
}

func NewProducer(brokers []string, topic string, codec *AvroCodec) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: false,
		},
		codec: codec,
	}
}

func (p *Producer) Publish(ctx context.Context, event domain.WarehouseEvent) error {
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if event.SchemaVersion == 0 {
		event.SchemaVersion = 2
	}
	value, err := p.codec.Encode(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.EventID),
		Value: value,
		Time:  event.OccurredAt,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

type DLQProducer struct {
	writer *kafka.Writer
}

func NewDLQProducer(brokers []string, topic string) *DLQProducer {
	return &DLQProducer{writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.LeastBytes{}, RequiredAcks: kafka.RequireAll}}
}

func (p *DLQProducer) Publish(ctx context.Context, original []byte, reason, code string, partition int, offset int64) error {
	payload := map[string]any{
		"original_event": base64.StdEncoding.EncodeToString(original),
		"error_reason":   reason,
		"error_code":     code,
		"failed_at":      time.Now().UTC().Format(time.RFC3339Nano),
		"kafka_metadata": map[string]interface{}{
			"partition": partition,
			"offset":    offset,
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{Value: data, Time: time.Now().UTC()})
}

func (p *DLQProducer) Close() error {
	return p.writer.Close()
}
