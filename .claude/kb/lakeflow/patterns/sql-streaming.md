# SQL Streaming and Advanced Features

> Source: https://docs.databricks.com/aws/en/dlt/sql-dev
> Lines: < 200

## Change Data Capture (CDC)

### SCD Type 1 (Current State)

```sql
CREATE OR REFRESH STREAMING TABLE customers;

APPLY CHANGES INTO
    customers
FROM
    STREAM customers_cdc
KEYS
    (customer_id)
SEQUENCE BY
    timestamp
COLUMNS * EXCEPT (operation, timestamp)
STORED AS
    SCD TYPE 1
```

### SCD Type 2 (Historical Tracking)

```sql
CREATE OR REFRESH STREAMING TABLE customers_history;

APPLY CHANGES INTO
    customers_history
FROM
    STREAM customers_cdc
KEYS
    (customer_id)
SEQUENCE BY
    timestamp
STORED AS
    SCD TYPE 2
```

## Advanced SQL Features

### Joins Between Tables

```sql
CREATE OR REFRESH MATERIALIZED VIEW order_details AS
SELECT
    o.order_id,
    o.order_date,
    c.customer_name,
    c.email
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
```

### Window Functions

```sql
CREATE OR REFRESH MATERIALIZED VIEW customer_rankings AS
SELECT
    customer_id,
    total_orders,
    RANK() OVER (ORDER BY total_orders DESC) as rank
FROM (
    SELECT
        customer_id,
        COUNT(*) as total_orders
    FROM orders
    GROUP BY customer_id
)
```

### Aggregations

```sql
CREATE OR REFRESH MATERIALIZED VIEW daily_sales AS
SELECT
    DATE(order_date) as sale_date,
    SUM(amount) as total_sales,
    COUNT(*) as order_count,
    AVG(amount) as avg_order_value
FROM orders
GROUP BY DATE(order_date)
```

## Limitations

### Not Supported

- **PIVOT clause** - Not supported in Lakeflow pipelines
- **CREATE OR REFRESH LIVE TABLE** - Deprecated syntax

### Key Differences from Traditional SQL

1. **Dataflow Graph Evaluation** - Evaluates dataset definitions across all source files before execution
2. **Streaming Semantics** - Different behavior for streaming vs batch tables
3. **Incremental Processing** - Automatically handles incremental data

## Best Practices

### DO

1. **Use STREAMING TABLE** for real-time data
2. **Use MATERIALIZED VIEW** for batch aggregations
3. **Apply expectations** for data quality
4. **Parameterize** pipeline configurations
5. **Document tables** with comments and properties
6. **Use read_files** for cloud storage ingestion

### DON'T

1. **Don't use PIVOT** (not supported)
2. **Don't mix deprecated syntax** (LIVE TABLE)
3. **Don't skip schema evolution** planning
4. **Don't ignore data quality** expectations

## Performance Tips

1. **Partition large tables** for better query performance
2. **Use Z-ordering** on commonly filtered columns
3. **Leverage Auto Loader** schema inference caching
4. **Monitor pipeline** update durations
5. **Optimize expectations** to avoid unnecessary data scans

## Related

- [SQL Table Syntax](sql-tables.md)
- [CDC Patterns](cdc-apply-changes.md)
- [Materialized Views Reference](../reference/materialized-views.md)
