package wms

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"smart-warehouse/internal/domain"
)

type Publisher interface {
	Publish(ctx context.Context, event domain.WarehouseEvent) error
}

type Generator struct {
	logger    *slog.Logger
	publisher Publisher
	interval  time.Duration
	runID     string
	tick      int64
	batchID   int64
	step      int
}

func NewGenerator(logger *slog.Logger, publisher Publisher, interval time.Duration) *Generator {
	return &Generator{
		logger:    logger,
		publisher: publisher,
		interval:  interval,
		runID:     fmt.Sprintf("%d", time.Now().UTC().UnixNano()),
	}
}

func (g *Generator) Run(ctx context.Context) error {
	if g.interval <= 0 {
		g.logger.Info("automatic event generation disabled")
		return nil
	}

	g.logger.Info("automatic event generation started", "interval", g.interval.String())
	if err := g.publishNext(ctx); err != nil {
		g.logger.Error("generate warehouse events", "error", err)
	}

	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := g.publishNext(ctx); err != nil {
				g.logger.Error("generate warehouse events", "error", err)
			}
		}
	}
}

func (g *Generator) publishNext(ctx context.Context) error {
	g.tick++
	if g.tick%12 == 0 {
		return g.publish(ctx, g.invalidEvent())
	}
	if g.batchID == 0 || g.step == 0 {
		g.batchID++
	}

	event := g.eventForStep()
	g.step = (g.step + 1) % 8
	return g.publish(ctx, event)
}

func (g *Generator) eventForStep() domain.WarehouseEvent {
	batchID := g.batchID
	productID := fmt.Sprintf("SKU-AUTO-%s-%06d", g.runID, batchID)
	orderID := fmt.Sprintf("ORDER-AUTO-%s-%06d", g.runID, batchID)
	supplierID := fmt.Sprintf("SUP-AUTO-%03d", batchID%1000)
	base := time.Now().UTC().Truncate(time.Millisecond)

	events := []domain.WarehouseEvent{
		{
			EventID:       fmt.Sprintf("auto-%s-%06d-01-received", g.runID, batchID),
			EventType:     domain.EventProductReceived,
			OccurredAt:    base,
			ProductID:     productID,
			ZoneID:        "ZONE-A",
			Quantity:      100,
			SupplierID:    &supplierID,
			SchemaVersion: 2,
		},
		{
			EventID:       fmt.Sprintf("auto-%s-%06d-02-reserved", g.runID, batchID),
			EventType:     domain.EventProductReserved,
			OccurredAt:    base.Add(time.Second),
			ProductID:     productID,
			ZoneID:        "ZONE-A",
			Quantity:      10,
			SchemaVersion: 2,
		},
		{
			EventID:       fmt.Sprintf("auto-%s-%06d-03-released", g.runID, batchID),
			EventType:     domain.EventProductReleased,
			OccurredAt:    base.Add(2 * time.Second),
			ProductID:     productID,
			ZoneID:        "ZONE-A",
			Quantity:      5,
			SchemaVersion: 2,
		},
		{
			EventID:       fmt.Sprintf("auto-%s-%06d-04-moved", g.runID, batchID),
			EventType:     domain.EventProductMoved,
			OccurredAt:    base.Add(3 * time.Second),
			ProductID:     productID,
			FromZoneID:    "ZONE-A",
			ToZoneID:      "ZONE-B",
			Quantity:      15,
			SchemaVersion: 2,
		},
		{
			EventID:       fmt.Sprintf("auto-%s-%06d-05-shipped", g.runID, batchID),
			EventType:     domain.EventProductShipped,
			OccurredAt:    base.Add(4 * time.Second),
			ProductID:     productID,
			ZoneID:        "ZONE-A",
			Quantity:      10,
			SchemaVersion: 2,
		},
		{
			EventID:         fmt.Sprintf("auto-%s-%06d-06-counted", g.runID, batchID),
			EventType:       domain.EventInventoryCounted,
			OccurredAt:      base.Add(5 * time.Second),
			ProductID:       productID,
			ZoneID:          "ZONE-A",
			CountedQuantity: 80,
			SchemaVersion:   2,
		},
		{
			EventID:       fmt.Sprintf("auto-%s-%06d-07-order-created", g.runID, batchID),
			EventType:     domain.EventOrderCreated,
			OccurredAt:    base.Add(6 * time.Second),
			OrderID:       orderID,
			Items:         []domain.OrderItem{{ProductID: productID, ZoneID: "ZONE-A", Quantity: 5}},
			SchemaVersion: 2,
		},
		{
			EventID:       fmt.Sprintf("auto-%s-%06d-08-order-completed", g.runID, batchID),
			EventType:     domain.EventOrderCompleted,
			OccurredAt:    base.Add(7 * time.Second),
			OrderID:       orderID,
			SchemaVersion: 2,
		},
	}
	return events[g.step]
}

func (g *Generator) invalidEvent() domain.WarehouseEvent {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return domain.WarehouseEvent{
		EventID:       fmt.Sprintf("auto-%s-invalid-%06d", g.runID, g.tick),
		EventType:     domain.EventProductShipped,
		OccurredAt:    now,
		ProductID:     fmt.Sprintf("SKU-AUTO-%s-invalid", g.runID),
		ZoneID:        "ZONE-A",
		Quantity:      -5,
		SchemaVersion: 2,
	}
}

func (g *Generator) publish(ctx context.Context, event domain.WarehouseEvent) error {
	if err := g.publisher.Publish(ctx, event); err != nil {
		return fmt.Errorf("publish %s: %w", event.EventID, err)
	}
	if event.Quantity < 0 || event.CountedQuantity < 0 {
		g.logger.Info("invalid warehouse event generated", "event_id", event.EventID, "event_type", event.EventType)
		return nil
	}
	g.logger.Info("warehouse event generated", "event_id", event.EventID, "event_type", event.EventType)
	return nil
}
