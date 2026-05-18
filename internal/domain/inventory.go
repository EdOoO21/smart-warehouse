package domain

import "time"

type Inventory struct {
	ProductID     string
	ZoneID        string
	Available     int
	Reserved      int
	SupplierID    *string
	LastEventTime time.Time
}

type ProductTotal struct {
	ProductID     string
	Available     int
	Reserved      int
	LastEventTime time.Time
}

type Order struct {
	OrderID string
	Status  string
	Items   []OrderItem
}
