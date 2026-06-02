package warehouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"smart-warehouse/internal/domain"
)

type Processor struct {
	repository Repository
	metrics    Metrics
	logger     *slog.Logger
}

func NewProcessor(logger *slog.Logger, repository Repository, metrics Metrics) *Processor {
	return &Processor{logger: logger, repository: repository, metrics: metrics}
}

func (p *Processor) Process(ctx context.Context, event domain.WarehouseEvent) error {
	start := time.Now()
	defer func() { p.metrics.ObserveDuration(time.Since(start).Seconds()) }()

	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate event: %w", err)
	}

	processed, err := p.repository.AlreadyProcessed(ctx, event.EventID)
	if err != nil {
		return fmt.Errorf("check processed event: %w", err)
	}
	if processed {
		return domain.ErrDuplicateEvent
	}

	entityKeys := event.EntityKeys()
	for _, key := range entityKeys {
		last, err := p.repository.LastEntityTimestamp(ctx, key)
		if err != nil {
			return fmt.Errorf("read entity timestamp %s: %w", key, err)
		}
		if !last.IsZero() && event.OccurredAt.Before(last) {
			return fmt.Errorf("%w: event time %s is older than %s for %s", domain.ErrStaleEvent, event.OccurredAt.Format(time.RFC3339), last.Format(time.RFC3339), key)
		}
	}

	inventories, order, err := p.buildState(ctx, event)
	if err != nil {
		return err
	}
	entityKeys = appendMissingInventoryKeys(entityKeys, inventories)
	for _, key := range entityKeys {
		last, err := p.repository.LastEntityTimestamp(ctx, key)
		if err != nil {
			return fmt.Errorf("read entity timestamp %s: %w", key, err)
		}
		if !last.IsZero() && event.OccurredAt.Before(last) {
			return fmt.Errorf("%w: event time %s is older than %s for %s", domain.ErrStaleEvent, event.OccurredAt.Format(time.RFC3339), last.Format(time.RFC3339), key)
		}
	}
	if err := p.repository.ApplyEvent(ctx, event, inventories, order, entityKeys); err != nil {
		p.metrics.IncCassandraWriteError()
		return fmt.Errorf("write event state: %w", err)
	}

	p.metrics.IncProcessed(string(event.EventType))
	return nil
}

func (p *Processor) buildState(ctx context.Context, event domain.WarehouseEvent) ([]domain.Inventory, *domain.Order, error) {
	switch event.EventType {
	case domain.EventProductReceived:
		inv, err := p.changeInventory(ctx, event.ProductID, event.ZoneID, event.Quantity, 0, event.OccurredAt, event.SupplierID)
		return single(inv, err)
	case domain.EventProductShipped:
		inv, err := p.changeInventory(ctx, event.ProductID, event.ZoneID, -event.Quantity, 0, event.OccurredAt, nil)
		return single(inv, err)
	case domain.EventProductReserved:
		inv, err := p.changeInventory(ctx, event.ProductID, event.ZoneID, -event.Quantity, event.Quantity, event.OccurredAt, nil)
		return single(inv, err)
	case domain.EventProductReleased:
		inv, err := p.changeInventory(ctx, event.ProductID, event.ZoneID, event.Quantity, -event.Quantity, event.OccurredAt, nil)
		return single(inv, err)
	case domain.EventInventoryCounted:
		inv, err := p.repository.GetInventory(ctx, event.ProductID, event.ZoneID)
		if err != nil {
			return nil, nil, err
		}
		inv.Available = event.CountedQuantity
		inv.LastEventTime = event.OccurredAt
		return []domain.Inventory{inv}, nil, nil
	case domain.EventProductMoved:
		from, err := p.changeInventory(ctx, event.ProductID, event.FromZoneID, -event.Quantity, 0, event.OccurredAt, nil)
		if err != nil {
			return nil, nil, err
		}
		to, err := p.changeInventory(ctx, event.ProductID, event.ToZoneID, event.Quantity, 0, event.OccurredAt, nil)
		if err != nil {
			return nil, nil, err
		}
		return []domain.Inventory{from, to}, nil, nil
	case domain.EventOrderCreated:
		inventories := make([]domain.Inventory, 0, len(event.Items))
		for _, item := range event.Items {
			inv, err := p.changeInventory(ctx, item.ProductID, item.ZoneID, -item.Quantity, item.Quantity, event.OccurredAt, nil)
			if err != nil {
				return nil, nil, err
			}
			inventories = append(inventories, inv)
		}
		order := &domain.Order{OrderID: event.OrderID, Status: "CREATED", Items: event.Items}
		return inventories, order, nil
	case domain.EventOrderCompleted:
		order, err := p.repository.GetOrder(ctx, event.OrderID)
		if err != nil {
			return nil, nil, err
		}
		if order.OrderID == "" {
			return nil, nil, fmt.Errorf("order %s not found", event.OrderID)
		}
		inventories := make([]domain.Inventory, 0, len(order.Items))
		for _, item := range order.Items {
			inv, err := p.changeInventory(ctx, item.ProductID, item.ZoneID, 0, -item.Quantity, event.OccurredAt, nil)
			if err != nil {
				return nil, nil, err
			}
			inventories = append(inventories, inv)
		}
		order.Status = "COMPLETED"
		return inventories, &order, nil
	default:
		return nil, nil, fmt.Errorf("unsupported event type %s", event.EventType)
	}
}

func (p *Processor) changeInventory(ctx context.Context, productID, zoneID string, availableDelta, reservedDelta int, occurredAt time.Time, supplierID *string) (domain.Inventory, error) {
	inv, err := p.repository.GetInventory(ctx, productID, zoneID)
	if err != nil {
		return domain.Inventory{}, err
	}
	inv.ProductID = productID
	inv.ZoneID = zoneID
	inv.Available += availableDelta
	inv.Reserved += reservedDelta
	if supplierID != nil {
		inv.SupplierID = supplierID
	}
	inv.LastEventTime = occurredAt
	if inv.Available < 0 {
		return domain.Inventory{}, fmt.Errorf("insufficient available inventory for %s/%s: %d", productID, zoneID, inv.Available)
	}
	if inv.Reserved < 0 {
		return domain.Inventory{}, fmt.Errorf("reserved inventory below zero for %s/%s: %d", productID, zoneID, inv.Reserved)
	}
	return inv, nil
}

func single(inv domain.Inventory, err error) ([]domain.Inventory, *domain.Order, error) {
	if err != nil {
		return nil, nil, err
	}
	return []domain.Inventory{inv}, nil, nil
}

func IsSkippable(err error) bool {
	return errors.Is(err, domain.ErrDuplicateEvent) || errors.Is(err, domain.ErrStaleEvent)
}

func appendMissingInventoryKeys(keys []string, inventories []domain.Inventory) []string {
	seen := make(map[string]struct{}, len(keys)+len(inventories))
	for _, key := range keys {
		seen[key] = struct{}{}
	}
	for _, inv := range inventories {
		key := domain.InventoryKey(inv.ProductID, inv.ZoneID)
		if _, ok := seen[key]; ok {
			continue
		}
		keys = append(keys, key)
		seen[key] = struct{}{}
	}
	return keys
}
