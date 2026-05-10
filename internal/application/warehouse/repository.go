package warehouse

import (
	"context"
	"time"

	"smart-warehouse/internal/domain"
)

type Repository interface {
	AlreadyProcessed(ctx context.Context, eventID string) (bool, error)
	LastEntityTimestamp(ctx context.Context, entityKey string) (time.Time, error)
	GetInventory(ctx context.Context, productID, zoneID string) (domain.Inventory, error)
	GetOrder(ctx context.Context, orderID string) (domain.Order, error)
	ApplyEvent(ctx context.Context, event domain.WarehouseEvent, inventories []domain.Inventory, order *domain.Order, entityKeys []string) error
	Ping(ctx context.Context) error
	Close()
}

type DLQPublisher interface {
	Publish(ctx context.Context, original []byte, reason, code string, partition int, offset int64) error
}

type Metrics interface {
	IncProcessed(eventType string)
	IncFailed(errorCode string)
	ObserveDuration(seconds float64)
	IncCassandraWriteError()
	SetLag(partition int, lag int64)
}
