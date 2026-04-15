# CDC Apply Changes Pattern

> Source: https://docs.databricks.com/aws/en/dlt/cdc
> Lines: < 200

## Overview

Change Data Capture (CDC) processes data changes from change data feeds or database snapshots, enabling incremental data processing and historical tracking.

## CDC APIs

| API | Purpose |
|-----|---------|
| AUTO CDC | Processes changes from a change data feed (CDF) |
| AUTO CDC FROM SNAPSHOT | Processes changes in database snapshots (Python only, Public Preview) |

## SCD Type 1 (Current State)

**Update records directly. History is not retained for updated records.**

### Python

```python
dlt.create_auto_cdc_flow(
    target="customers",
    source="customers_cdc_stream",
    keys=["customer_id"],
    sequence_by=col("timestamp"),
    stored_as_scd_type=1
)
```

### SQL

```sql
CREATE OR REFRESH STREAMING TABLE customers;

APPLY CHANGES INTO customers
FROM STREAM customers_cdc
KEYS (customer_id)
SEQUENCE BY timestamp
COLUMNS * EXCEPT (operation, timestamp)
STORED AS SCD TYPE 1
```

**Use Cases:**
- Current state tracking
- No historical analysis needed
- Storage optimization

## SCD Type 2 (Historical)

**Retain a history of records on all updates or on updates to a specified set of columns**

### Python

```python
dlt.create_auto_cdc_flow(
    target="customers_history",
    source="customers_cdc_stream",
    keys=["customer_id"],
    sequence_by=col("timestamp"),
    stored_as_scd_type=2
)
```

### SQL

```sql
CREATE OR REFRESH STREAMING TABLE customers_history;

APPLY CHANGES INTO customers_history
FROM STREAM customers_cdc
KEYS (customer_id)
SEQUENCE BY timestamp
STORED AS SCD TYPE 2
```

**Generated Columns:**
- `__START_AT`: When record became active
- `__END_AT`: When record became inactive (NULL for current)
- `__IS_CURRENT`: Boolean flag for current record

**Use Cases:**
- Audit trails
- Historical analysis
- Point-in-time queries
- Compliance requirements

## Sequencing Requirements

### Single Column

```python
sequence_by=col("timestamp")
```

### Multiple Columns

```python
sequence_by=struct("date", "sequence_number")
```

### Rules

| DO | DON'T |
|----|-------|
| Use sortable data types (timestamps, integers, dates) | Use NULL sequencing values (unsupported) |
| Ensure one distinct update per key at each sequencing value | Have duplicate sequence values for the same key |
| Handle timezone consistency for timestamps | Mix timezone-aware and timezone-naive timestamps |

## Advanced Options

### Delete Handling

```python
apply_as_deletes=F.expr("operation = 'DELETE'")
```

### Track Specific Columns (SCD Type 2)

```python
dlt.apply_changes(
    target="users_history",
    source="users_cdc_silver",
    keys=["user_id"],
    sequence_by="sequence_num",
    track_history_column_list=["email", "phone"],
    stored_as_scd_type=2
)
```

### Exclude Columns

```python
except_column_list=["operation", "timestamp", "source_system"]
```

## Requirements

- **Edition**: Serverless Lakeflow or Pro/Advanced editions
- **Sequencing column**: Must be sortable data type
- **Keys**: Must uniquely identify records

## Related

- [CDC Complete Example](cdc-example.md)
- [Python Streaming](python-streaming.md)
- [SQL Streaming](sql-streaming.md)
