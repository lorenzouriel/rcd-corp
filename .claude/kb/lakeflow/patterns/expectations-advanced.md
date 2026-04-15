# Advanced Data Quality Expectations

> Source: https://docs.databricks.com/aws/en/dlt/expectations
> Lines: < 200

## Grouped Expectations (Python)

### expect_all (WARN)

```python
@dlt.expect_all({
    "valid_id": "customer_id IS NOT NULL",
    "valid_email": "email LIKE '%@%.%'",
    "positive_age": "age > 0 AND age < 150"
})
@dlt.table()
def customers_warn():
    return spark.read.table("raw_customers")
```

### expect_all_or_drop (DROP)

```python
@dlt.expect_all_or_drop({
    "valid_id": "customer_id IS NOT NULL",
    "valid_email": "email LIKE '%@%.%'"
})
@dlt.table()
def customers_drop():
    return spark.read.table("raw_customers")
```

### expect_all_or_fail (FAIL)

```python
@dlt.expect_all_or_fail({
    "critical_id": "customer_id IS NOT NULL"
})
@dlt.table()
def customers_fail():
    return spark.read.table("raw_customers")
```

## Complete Multi-Layer Example

```python
import dlt
from pyspark.sql import functions as F

# Bronze: Warn only, keep all data
@dlt.expect("has_data", "_rescued_data IS NULL")
@dlt.table(
    comment="Raw ingestion with quality warnings",
    table_properties={"quality": "bronze"}
)
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://bucket/orders/")
    )

# Silver: Drop invalid records
@dlt.expect_all_or_drop({
    "valid_id": "order_id IS NOT NULL",
    "valid_date": "order_date IS NOT NULL",
    "positive_amount": "amount > 0",
    "valid_status": "status IN ('PENDING', 'COMPLETED', 'CANCELLED')",
    "recent_order": "order_date >= '2020-01-01'"
})
@dlt.table(
    comment="Cleaned orders with quality checks",
    table_properties={"quality": "silver"}
)
def orders_silver():
    return dlt.read_stream("orders_bronze")

# Gold: Fail on critical violations
@dlt.expect_or_fail("revenue_integrity", "total_revenue >= 0")
@dlt.table(
    comment="Aggregated revenue with strict quality",
    table_properties={"quality": "gold"}
)
def daily_revenue():
    return (
        spark.read.table("orders_silver")
        .groupBy(F.date_trunc("day", "order_date").alias("date"))
        .agg(F.sum("amount").alias("total_revenue"))
    )
```

## Monitoring Expectations

### View Metrics in UI
- Pipeline dashboard shows expectation violations
- Drill down to see specific failed records
- Track trends over time

### Access Metrics Programmatically

```python
events = spark.read.format("delta").load("/event_log_path")
expectations = events.filter("event_type = 'flow_progress'")
```

## Common Patterns

### Not Null Checks
```python
@dlt.expect_or_drop("not_null", "critical_column IS NOT NULL")
```

### Range Validation
```python
@dlt.expect_or_drop("valid_range", "age BETWEEN 0 AND 150")
```

### Format Validation
```python
@dlt.expect_or_drop("valid_email", "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'")
```

### Referential Integrity (via join)
```python
@dlt.table()
def orders_with_valid_customers():
    return (
        dlt.read("orders")
        .join(dlt.read("customers"), "customer_id", "inner")
    )
```

### Date Validation
```python
@dlt.expect_or_drop("valid_date_range", "start_date <= end_date")
@dlt.expect_or_drop("recent_data", "created_at >= current_date() - INTERVAL 1 YEAR")
```

## Best Practices

### DO

1. **Layer quality checks**: Bronze=WARN, Silver=DROP, Gold=FAIL
2. **Name expectations clearly**: Use descriptive names
3. **Test expectations**: Validate with sample data
4. **Balance strictness**: Too strict = pipeline failures
5. **Document quality rules**: Explain business rationale

### DON'T

1. **Don't fail everything**: Use FAIL sparingly
2. **Don't ignore warnings**: Review metrics regularly
3. **Don't create circular dependencies**: Avoid referencing downstream tables
4. **Don't overcomplicate**: Keep rules simple and readable

## Limitations

- Only supported in streaming tables and materialized views
- Metrics not available in certain scenarios
- Cannot reference other tables in constraints (use joins first)

## Performance Considerations

- Expectations are evaluated during data processing
- Complex constraints may impact pipeline performance
- Consider computational cost of SQL functions
- Use indexed columns when possible

## Related

- [Basic Expectations](expectations.md)
- [Python Decorators](python-decorators.md)
- [SQL Tables](sql-tables.md)
