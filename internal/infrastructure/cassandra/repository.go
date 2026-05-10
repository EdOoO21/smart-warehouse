package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"smart-warehouse/internal/domain"
)

type Repository struct {
	session *gocql.Session
	readCL  gocql.Consistency
	writeCL gocql.Consistency
}

func NewRepository(hosts []string, keyspace, readConsistency, writeConsistency string) (*Repository, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = parseConsistency(readConsistency, gocql.Quorum)
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &Repository{
		session: session,
		readCL:  parseConsistency(readConsistency, gocql.Quorum),
		writeCL: parseConsistency(writeConsistency, gocql.Quorum),
	}, nil
}

func (r *Repository) Close() {
	r.session.Close()
}

func (r *Repository) Ping(ctx context.Context) error {
	return r.session.Query("SELECT now() FROM system.local").WithContext(ctx).Consistency(r.readCL).Exec()
}

func (r *Repository) AlreadyProcessed(ctx context.Context, eventID string) (bool, error) {
	var existing string
	err := r.session.Query("SELECT event_id FROM processed_events WHERE event_id = ?", eventID).
		WithContext(ctx).Consistency(r.readCL).Scan(&existing)
	if err == nil {
		return true, nil
	}
	if err == gocql.ErrNotFound {
		return false, nil
	}
	return false, err
}

func (r *Repository) LastEntityTimestamp(ctx context.Context, entityKey string) (time.Time, error) {
	var ts time.Time
	err := r.session.Query("SELECT last_event_timestamp FROM entity_versions WHERE entity_key = ?", entityKey).
		WithContext(ctx).Consistency(r.readCL).Scan(&ts)
	if err == gocql.ErrNotFound {
		return time.Time{}, nil
	}
	return ts, err
}

func (r *Repository) GetInventory(ctx context.Context, productID, zoneID string) (domain.Inventory, error) {
	var inv domain.Inventory
	var supplierID *string
	var last time.Time
	err := r.session.Query(`SELECT product_id, zone_id, available_quantity, reserved_quantity, supplier_id, last_event_timestamp
		FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?`, productID, zoneID).
		WithContext(ctx).Consistency(r.readCL).
		Scan(&inv.ProductID, &inv.ZoneID, &inv.Available, &inv.Reserved, &supplierID, &last)
	if err == gocql.ErrNotFound {
		return domain.Inventory{ProductID: productID, ZoneID: zoneID}, nil
	}
	if err != nil {
		return domain.Inventory{}, err
	}
	inv.SupplierID = supplierID
	inv.LastEventTime = last
	return inv, nil
}

func (r *Repository) GetOrder(ctx context.Context, orderID string) (domain.Order, error) {
	var order domain.Order
	var itemsJSON string
	err := r.session.Query("SELECT order_id, status, items_json FROM orders_by_id WHERE order_id = ?", orderID).
		WithContext(ctx).Consistency(r.readCL).Scan(&order.OrderID, &order.Status, &itemsJSON)
	if err == gocql.ErrNotFound {
		return domain.Order{}, nil
	}
	if err != nil {
		return domain.Order{}, err
	}
	if itemsJSON != "" {
		if err := json.Unmarshal([]byte(itemsJSON), &order.Items); err != nil {
			return domain.Order{}, err
		}
	}
	return order, nil
}

func (r *Repository) ApplyEvent(ctx context.Context, event domain.WarehouseEvent, inventories []domain.Inventory, order *domain.Order, entityKeys []string) error {
	productIDs := uniqueProducts(inventories)
	totals := make(map[string]domain.ProductTotal, len(productIDs))
	for _, productID := range productIDs {
		total, err := r.calculateProductTotal(ctx, productID, inventories)
		if err != nil {
			return err
		}
		totals[productID] = total
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return err
	}

	batch := r.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Cons = r.writeCL
	for _, inv := range inventories {
		batch.Query(`INSERT INTO inventory_by_product_zone
			(product_id, zone_id, available_quantity, reserved_quantity, supplier_id, last_event_id, last_event_timestamp)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			inv.ProductID, inv.ZoneID, inv.Available, inv.Reserved, inv.SupplierID, event.EventID, event.OccurredAt)
		batch.Query(`INSERT INTO inventory_by_zone
			(zone_id, product_id, available_quantity, reserved_quantity, supplier_id, last_event_id, last_event_timestamp)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			inv.ZoneID, inv.ProductID, inv.Available, inv.Reserved, inv.SupplierID, event.EventID, event.OccurredAt)
	}
	for _, total := range totals {
		batch.Query(`INSERT INTO inventory_by_product
			(product_id, total_available_quantity, total_reserved_quantity, last_event_id, last_event_timestamp)
			VALUES (?, ?, ?, ?, ?)`,
			total.ProductID, total.Available, total.Reserved, event.EventID, event.OccurredAt)
	}
	for _, key := range entityKeys {
		batch.Query("INSERT INTO entity_versions (entity_key, last_event_id, last_event_timestamp) VALUES (?, ?, ?)", key, event.EventID, event.OccurredAt)
	}
	if order != nil {
		itemsJSON, err := json.Marshal(order.Items)
		if err != nil {
			return err
		}
		batch.Query("INSERT INTO orders_by_id (order_id, status, items_json, updated_at) VALUES (?, ?, ?, ?)", order.OrderID, order.Status, string(itemsJSON), event.OccurredAt)
	}
	batch.Query("INSERT INTO processed_events (event_id, event_type, processed_at) VALUES (?, ?, ?)", event.EventID, string(event.EventType), time.Now().UTC())
	batch.Query(`INSERT INTO event_history (bucket, occurred_at, event_id, event_type, event_json)
		VALUES (?, ?, ?, ?, ?)`, event.OccurredAt.Format("2006-01-02"), event.OccurredAt, event.EventID, string(event.EventType), string(eventJSON))

	return r.session.ExecuteBatch(batch)
}

func (r *Repository) calculateProductTotal(ctx context.Context, productID string, changed []domain.Inventory) (domain.ProductTotal, error) {
	byZone := map[string]domain.Inventory{}
	iter := r.session.Query("SELECT zone_id, available_quantity, reserved_quantity FROM inventory_by_product_zone WHERE product_id = ?", productID).
		WithContext(ctx).Consistency(r.readCL).Iter()
	var zoneID string
	var available, reserved int
	for iter.Scan(&zoneID, &available, &reserved) {
		byZone[zoneID] = domain.Inventory{ProductID: productID, ZoneID: zoneID, Available: available, Reserved: reserved}
	}
	if err := iter.Close(); err != nil {
		return domain.ProductTotal{}, err
	}
	for _, inv := range changed {
		if inv.ProductID == productID {
			byZone[inv.ZoneID] = inv
		}
	}
	total := domain.ProductTotal{ProductID: productID}
	for _, inv := range byZone {
		total.Available += inv.Available
		total.Reserved += inv.Reserved
	}
	return total, nil
}

func uniqueProducts(inventories []domain.Inventory) []string {
	seen := map[string]struct{}{}
	var productIDs []string
	for _, inv := range inventories {
		if _, ok := seen[inv.ProductID]; ok {
			continue
		}
		seen[inv.ProductID] = struct{}{}
		productIDs = append(productIDs, inv.ProductID)
	}
	return productIDs
}

func parseConsistency(value string, fallback gocql.Consistency) gocql.Consistency {
	switch strings.ToUpper(value) {
	case "ONE":
		return gocql.One
	case "QUORUM":
		return gocql.Quorum
	case "ALL":
		return gocql.All
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum
	default:
		return fallback
	}
}

func CreateKeyspace(hosts []string, keyspace string) error {
	cluster := gocql.NewCluster(hosts...)
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 10}
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()
	return session.Query(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s
		WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3}
		AND durable_writes = true`, keyspace)).Exec()
}
