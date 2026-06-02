package kafka

import (
	"context"
	"testing"
	"time"

	"smart-warehouse/internal/domain"
)

func TestAvroCodecEncodesV1AndV2(t *testing.T) {
	codec, err := NewAvroCodec(nil)
	if err != nil {
		t.Fatal(err)
	}
	codec.v1ID = 1
	codec.v2ID = 2
	codec.ids[1] = codec.v1
	codec.ids[2] = codec.v2
	supplier := "SUP-001"

	tests := []domain.WarehouseEvent{
		{
			EventID:       "v1",
			EventType:     domain.EventProductReceived,
			OccurredAt:    time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
			ProductID:     "SKU-1",
			ZoneID:        "ZONE-A",
			Quantity:      10,
			SchemaVersion: 1,
		},
		{
			EventID:       "v2",
			EventType:     domain.EventProductReceived,
			OccurredAt:    time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
			ProductID:     "SKU-2",
			ZoneID:        "ZONE-A",
			Quantity:      10,
			SupplierID:    &supplier,
			SchemaVersion: 2,
		},
	}

	for _, tt := range tests {
		data, err := codec.Encode(tt)
		if err != nil {
			t.Fatalf("encode %s: %v", tt.EventID, err)
		}
		got, err := codec.Decode(context.Background(), data)
		if err != nil {
			t.Fatalf("decode %s: %v", tt.EventID, err)
		}
		if got.EventID != tt.EventID || got.SchemaVersion != tt.SchemaVersion || got.Quantity != tt.Quantity || !got.OccurredAt.Equal(tt.OccurredAt) {
			t.Fatalf("unexpected decoded event: %+v", got)
		}
		if tt.SchemaVersion == 2 && (got.SupplierID == nil || *got.SupplierID != supplier) {
			t.Fatalf("supplier_id was not preserved: %+v", got)
		}
	}
}
