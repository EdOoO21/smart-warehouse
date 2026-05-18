package domain

import (
	"errors"
	"fmt"
	"time"
)

type EventType string

const (
	EventProductReceived  EventType = "PRODUCT_RECEIVED"
	EventProductShipped   EventType = "PRODUCT_SHIPPED"
	EventProductMoved     EventType = "PRODUCT_MOVED"
	EventProductReserved  EventType = "PRODUCT_RESERVED"
	EventProductReleased  EventType = "PRODUCT_RELEASED"
	EventInventoryCounted EventType = "INVENTORY_COUNTED"
	EventOrderCreated     EventType = "ORDER_CREATED"
	EventOrderCompleted   EventType = "ORDER_COMPLETED"
)

type OrderItem struct {
	ProductID string `json:"product_id"`
	ZoneID    string `json:"zone_id"`
	Quantity  int    `json:"quantity"`
}

type WarehouseEvent struct {
	EventID         string      `json:"event_id"`
	EventType       EventType   `json:"event_type"`
	OccurredAt      time.Time   `json:"occurred_at"`
	ProductID       string      `json:"product_id,omitempty"`
	ZoneID          string      `json:"zone_id,omitempty"`
	FromZoneID      string      `json:"from_zone_id,omitempty"`
	ToZoneID        string      `json:"to_zone_id,omitempty"`
	Quantity        int         `json:"quantity,omitempty"`
	CountedQuantity int         `json:"counted_quantity,omitempty"`
	OrderID         string      `json:"order_id,omitempty"`
	Items           []OrderItem `json:"items,omitempty"`
	SupplierID      *string     `json:"supplier_id,omitempty"`
	SchemaVersion   int         `json:"schema_version"`
}

func (e WarehouseEvent) Validate() error {
	if e.EventID == "" {
		return errors.New("event_id is required")
	}
	if e.OccurredAt.IsZero() {
		return errors.New("occurred_at is required")
	}
	switch e.EventType {
	case EventProductReceived, EventProductShipped, EventProductReserved, EventProductReleased:
		if e.ProductID == "" || e.ZoneID == "" {
			return fmt.Errorf("%s requires product_id and zone_id", e.EventType)
		}
		if e.Quantity <= 0 {
			return fmt.Errorf("invalid quantity: %d (must be positive)", e.Quantity)
		}
	case EventProductMoved:
		if e.ProductID == "" || e.FromZoneID == "" || e.ToZoneID == "" {
			return errors.New("PRODUCT_MOVED requires product_id, from_zone_id and to_zone_id")
		}
		if e.Quantity <= 0 {
			return fmt.Errorf("invalid quantity: %d (must be positive)", e.Quantity)
		}
	case EventInventoryCounted:
		if e.ProductID == "" || e.ZoneID == "" {
			return errors.New("INVENTORY_COUNTED requires product_id and zone_id")
		}
		if e.CountedQuantity < 0 {
			return fmt.Errorf("invalid counted_quantity: %d (must not be negative)", e.CountedQuantity)
		}
	case EventOrderCreated:
		if e.OrderID == "" || len(e.Items) == 0 {
			return errors.New("ORDER_CREATED requires order_id and items")
		}
		for _, item := range e.Items {
			if item.ProductID == "" || item.ZoneID == "" || item.Quantity <= 0 {
				return fmt.Errorf("invalid order item: %+v", item)
			}
		}
	case EventOrderCompleted:
		if e.OrderID == "" {
			return errors.New("ORDER_COMPLETED requires order_id")
		}
	default:
		return fmt.Errorf("unsupported event_type %q", e.EventType)
	}
	return nil
}

func (e WarehouseEvent) EntityKeys() []string {
	switch e.EventType {
	case EventProductMoved:
		return []string{InventoryKey(e.ProductID, e.FromZoneID), InventoryKey(e.ProductID, e.ToZoneID)}
	case EventOrderCreated:
		keys := make([]string, 0, len(e.Items)+1)
		keys = append(keys, "order:"+e.OrderID)
		for _, item := range e.Items {
			keys = append(keys, InventoryKey(item.ProductID, item.ZoneID))
		}
		return keys
	case EventOrderCompleted:
		return []string{"order:" + e.OrderID}
	default:
		if e.ProductID != "" && e.ZoneID != "" {
			return []string{InventoryKey(e.ProductID, e.ZoneID)}
		}
		return nil
	}
}

func InventoryKey(productID, zoneID string) string {
	return "inventory:" + productID + ":" + zoneID
}
