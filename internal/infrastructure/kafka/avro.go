package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"

	"smart-warehouse/internal/domain"
	"smart-warehouse/internal/infrastructure/schemaregistry"
)

const warehouseSubject = "warehouse-events-value"

const warehouseEventSchemaV1 = `{
  "type":"record",
  "name":"WarehouseEvent",
  "namespace":"smart.warehouse",
  "fields":[
    {"name":"event_id","type":"string"},
    {"name":"event_type","type":"string"},
    {"name":"occurred_at","type":{"type":"long","logicalType":"timestamp-millis"}},
    {"name":"product_id","type":["null","string"],"default":null},
    {"name":"zone_id","type":["null","string"],"default":null},
    {"name":"from_zone_id","type":["null","string"],"default":null},
    {"name":"to_zone_id","type":["null","string"],"default":null},
    {"name":"quantity","type":["null","int"],"default":null},
    {"name":"counted_quantity","type":["null","int"],"default":null},
    {"name":"order_id","type":["null","string"],"default":null},
    {"name":"items_json","type":["null","string"],"default":null}
  ]
}`

const warehouseEventSchemaV2 = `{
  "type":"record",
  "name":"WarehouseEvent",
  "namespace":"smart.warehouse",
  "fields":[
    {"name":"event_id","type":"string"},
    {"name":"event_type","type":"string"},
    {"name":"occurred_at","type":{"type":"long","logicalType":"timestamp-millis"}},
    {"name":"product_id","type":["null","string"],"default":null},
    {"name":"zone_id","type":["null","string"],"default":null},
    {"name":"from_zone_id","type":["null","string"],"default":null},
    {"name":"to_zone_id","type":["null","string"],"default":null},
    {"name":"quantity","type":["null","int"],"default":null},
    {"name":"counted_quantity","type":["null","int"],"default":null},
    {"name":"order_id","type":["null","string"],"default":null},
    {"name":"items_json","type":["null","string"],"default":null},
    {"name":"supplier_id","type":["null","string"],"default":null}
  ]
}`

type AvroCodec struct {
	registry *schemaregistry.Client
	mu       sync.Mutex
	ids      map[int]*goavro.Codec
	v1ID     int
	v2ID     int
	v1       *goavro.Codec
	v2       *goavro.Codec
}

func NewAvroCodec(registry *schemaregistry.Client) (*AvroCodec, error) {
	v1, err := goavro.NewCodec(warehouseEventSchemaV1)
	if err != nil {
		return nil, err
	}
	v2, err := goavro.NewCodec(warehouseEventSchemaV2)
	if err != nil {
		return nil, err
	}
	return &AvroCodec{registry: registry, ids: map[int]*goavro.Codec{}, v1: v1, v2: v2}, nil
}

func (c *AvroCodec) RegisterSchemas(ctx context.Context) error {
	v1ID, err := c.registry.Register(ctx, warehouseSubject, warehouseEventSchemaV1)
	if err != nil {
		return err
	}
	if err := c.registry.SetCompatibility(ctx, warehouseSubject, "BACKWARD"); err != nil {
		return err
	}
	v2ID, err := c.registry.Register(ctx, warehouseSubject, warehouseEventSchemaV2)
	if err != nil {
		return err
	}
	c.v1ID = v1ID
	c.v2ID = v2ID
	c.ids[v1ID] = c.v1
	c.ids[v2ID] = c.v2
	return nil
}

func (c *AvroCodec) Encode(event domain.WarehouseEvent) ([]byte, error) {
	codec := c.v2
	schemaID := c.v2ID
	if event.SchemaVersion == 1 {
		codec = c.v1
		schemaID = c.v1ID
	}
	if schemaID == 0 {
		return nil, fmt.Errorf("schema id is not registered")
	}

	native, err := eventToNative(event)
	if err != nil {
		return nil, err
	}
	if event.SchemaVersion == 1 {
		delete(native, "supplier_id")
	}
	payload, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.WriteByte(0)
	if err := binary.Write(&out, binary.BigEndian, int32(schemaID)); err != nil {
		return nil, err
	}
	out.Write(payload)
	return out.Bytes(), nil
}

func (c *AvroCodec) Decode(ctx context.Context, data []byte) (domain.WarehouseEvent, error) {
	if len(data) < 5 || data[0] != 0 {
		return domain.WarehouseEvent{}, fmt.Errorf("invalid confluent avro payload")
	}
	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	codec, err := c.codec(ctx, schemaID)
	if err != nil {
		return domain.WarehouseEvent{}, err
	}
	native, _, err := codec.NativeFromBinary(data[5:])
	if err != nil {
		return domain.WarehouseEvent{}, err
	}
	record, ok := native.(map[string]interface{})
	if !ok {
		return domain.WarehouseEvent{}, fmt.Errorf("decoded avro value is not a record")
	}
	event, err := nativeToEvent(record)
	if err != nil {
		return domain.WarehouseEvent{}, err
	}
	if schemaID == c.v1ID {
		event.SchemaVersion = 1
	} else {
		event.SchemaVersion = 2
	}
	return event, nil
}

func (c *AvroCodec) codec(ctx context.Context, schemaID int) (*goavro.Codec, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if codec, ok := c.ids[schemaID]; ok {
		return codec, nil
	}
	schema, err := c.registry.SchemaByID(ctx, schemaID)
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	c.ids[schemaID] = codec
	return codec, nil
}

func eventToNative(event domain.WarehouseEvent) (map[string]interface{}, error) {
	itemsJSON, err := json.Marshal(event.Items)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"event_id":         event.EventID,
		"event_type":       string(event.EventType),
		"occurred_at":      event.OccurredAt.UnixMilli(),
		"product_id":       nullableString(event.ProductID),
		"zone_id":          nullableString(event.ZoneID),
		"from_zone_id":     nullableString(event.FromZoneID),
		"to_zone_id":       nullableString(event.ToZoneID),
		"quantity":         nullableInt(event.Quantity),
		"counted_quantity": nullableInt(event.CountedQuantity),
		"order_id":         nullableString(event.OrderID),
		"items_json":       map[string]interface{}{"string": string(itemsJSON)},
		"supplier_id":      nullableStringPtr(event.SupplierID),
	}, nil
}

func nativeToEvent(record map[string]interface{}) (domain.WarehouseEvent, error) {
	event := domain.WarehouseEvent{
		EventID:         asString(record["event_id"]),
		EventType:       domain.EventType(asString(record["event_type"])),
		OccurredAt:      time.UnixMilli(asLong(record["occurred_at"])).UTC(),
		ProductID:       unionString(record["product_id"]),
		ZoneID:          unionString(record["zone_id"]),
		FromZoneID:      unionString(record["from_zone_id"]),
		ToZoneID:        unionString(record["to_zone_id"]),
		Quantity:        unionInt(record["quantity"]),
		CountedQuantity: unionInt(record["counted_quantity"]),
		OrderID:         unionString(record["order_id"]),
	}
	if supplierID := unionString(record["supplier_id"]); supplierID != "" {
		event.SupplierID = &supplierID
	}
	itemsJSON := unionString(record["items_json"])
	if itemsJSON != "" && itemsJSON != "null" {
		if err := json.Unmarshal([]byte(itemsJSON), &event.Items); err != nil {
			return domain.WarehouseEvent{}, err
		}
	}
	return event, nil
}

func nullableString(value string) interface{} {
	if value == "" {
		return nil
	}
	return map[string]interface{}{"string": value}
}

func nullableStringPtr(value *string) interface{} {
	if value == nil || *value == "" {
		return nil
	}
	return map[string]interface{}{"string": *value}
}

func nullableInt(value int) interface{} {
	if value == 0 {
		return nil
	}
	return map[string]interface{}{"int": value}
}

func asString(value interface{}) string {
	if value == nil {
		return ""
	}
	s, _ := value.(string)
	return s
}

func asLong(value interface{}) int64 {
	switch v := value.(type) {
	case time.Time:
		return v.UnixMilli()
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func unionString(value interface{}) string {
	m, ok := value.(map[string]interface{})
	if !ok {
		return ""
	}
	return asString(m["string"])
}

func unionInt(value interface{}) int {
	m, ok := value.(map[string]interface{})
	if !ok {
		return 0
	}
	switch v := m["int"].(type) {
	case int32:
		return int(v)
	case int:
		return v
	case float64:
		return int(v)
	default:
		return 0
	}
}
