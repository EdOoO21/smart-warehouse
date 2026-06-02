package warehouse

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"smart-warehouse/internal/domain"
)

func TestProcessorAppliesOrderScenario(t *testing.T) {
	repo := newMemoryRepository()
	processor := NewProcessor(slog.Default(), repo, &noopMetrics{})
	ctx := context.Background()
	now := time.Date(2026, 5, 28, 17, 43, 21, 0, time.UTC)

	events := []domain.WarehouseEvent{
		{EventID: "received", EventType: domain.EventProductReceived, OccurredAt: now, ProductID: "SKU-1", ZoneID: "ZONE-A", Quantity: 100},
		{EventID: "created", EventType: domain.EventOrderCreated, OccurredAt: now.Add(time.Second), OrderID: "ORD-1", Items: []domain.OrderItem{{ProductID: "SKU-1", ZoneID: "ZONE-A", Quantity: 15}}},
		{EventID: "completed", EventType: domain.EventOrderCompleted, OccurredAt: now.Add(2 * time.Second), OrderID: "ORD-1"},
	}
	for _, event := range events {
		if err := processor.Process(ctx, event); err != nil {
			t.Fatalf("process %s: %v", event.EventID, err)
		}
	}

	inv, err := repo.GetInventory(ctx, "SKU-1", "ZONE-A")
	if err != nil {
		t.Fatal(err)
	}
	if inv.Available != 85 || inv.Reserved != 0 {
		t.Fatalf("unexpected inventory: %+v", inv)
	}
	order, err := repo.GetOrder(ctx, "ORD-1")
	if err != nil {
		t.Fatal(err)
	}
	if order.Status != "COMPLETED" {
		t.Fatalf("unexpected order: %+v", order)
	}
}

type memoryRepository struct {
	processed map[string]struct{}
	versions  map[string]time.Time
	inventory map[string]domain.Inventory
	orders    map[string]domain.Order
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{
		processed: map[string]struct{}{},
		versions:  map[string]time.Time{},
		inventory: map[string]domain.Inventory{},
		orders:    map[string]domain.Order{},
	}
}

func (r *memoryRepository) AlreadyProcessed(_ context.Context, eventID string) (bool, error) {
	_, ok := r.processed[eventID]
	return ok, nil
}

func (r *memoryRepository) LastEntityTimestamp(_ context.Context, entityKey string) (time.Time, error) {
	return r.versions[entityKey], nil
}

func (r *memoryRepository) GetInventory(_ context.Context, productID, zoneID string) (domain.Inventory, error) {
	inv, ok := r.inventory[domain.InventoryKey(productID, zoneID)]
	if !ok {
		return domain.Inventory{ProductID: productID, ZoneID: zoneID}, nil
	}
	return inv, nil
}

func (r *memoryRepository) GetOrder(_ context.Context, orderID string) (domain.Order, error) {
	return r.orders[orderID], nil
}

func (r *memoryRepository) ApplyEvent(_ context.Context, event domain.WarehouseEvent, inventories []domain.Inventory, order *domain.Order, entityKeys []string) error {
	for _, inv := range inventories {
		r.inventory[domain.InventoryKey(inv.ProductID, inv.ZoneID)] = inv
	}
	if order != nil {
		r.orders[order.OrderID] = *order
	}
	for _, key := range entityKeys {
		r.versions[key] = event.OccurredAt
	}
	r.processed[event.EventID] = struct{}{}
	return nil
}

func (r *memoryRepository) Ping(context.Context) error { return nil }
func (r *memoryRepository) Close()                     {}

type noopMetrics struct{}

func (m *noopMetrics) IncProcessed(string)                             {}
func (m *noopMetrics) IncFailed(string)                                {}
func (m *noopMetrics) ObserveDuration(float64)                         {}
func (m *noopMetrics) ObserveHTTPRequest(string, string, int, float64) {}
func (m *noopMetrics) IncCassandraWriteError()                         {}
func (m *noopMetrics) SetLag(int, int64)                               {}
