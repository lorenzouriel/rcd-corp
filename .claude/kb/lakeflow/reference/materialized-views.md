# Materialized Views Optimization in Lakeflow

> Source: https://docs.databricks.com/aws/en/dlt/materialized-views

## Overview

Materialized views are cached query results that automatically track and incrementally process changes in upstream data, providing fast query performance with correct results.

## Key Characteristics

### What are Materialized Views?

**Materialized views are:**
- Cached query results that refresh at specified intervals
- Automatically track changes in upstream data
- Incrementally updated to maintain sync with source data
- Stored in Unity Catalog with metadata about the query

**Benefits:**
- ⚡ Faster query performance vs standard views
- ✅ Always provide correct results (even with late/out-of-order data)
- 💰 Minimize update costs through incremental processing
- 🔄 Automatic refresh on pipeline updates

## How They Work

### Storage and Metadata

```python
@dlt.table(
    comment="Regional sales materialized view",
    table_properties={
        "quality": "gold",
        "refresh_strategy": "incremental"
    }
)
def regional_sales():
    partners_df = spark.read.table("partners")
    sales_df = spark.read.table("sales")

    return (
        partners_df
        .join(sales_df, on="partner_id", how="inner")
        .groupBy("region", "product_category")
        .agg(
            F.sum("amount").alias("total_sales"),
            F.count("*").alias("transaction_count")
        )
    )
```

**What happens:**
1. Query definition stored in Unity Catalog
2. Results materialized in cloud storage (Delta tables)
3. Metadata tracks source dependencies
4. Refresh logic determines incremental updates

### Incremental Update Process

**Databricks attempts to process only the data that must be processed:**

```
Initial state:
partners: 1000 rows
sales: 10000 rows
→ Materialized view: Full computation

Update:
partners: +10 new rows
sales: +100 new rows
→ Materialized view: Process only affected rows (~110 rows)
```

**Not all operations support incremental refresh:**
- ✅ Filters, projections
- ✅ Joins (with keys)
- ✅ Aggregations (with group by)
- ❌ Window functions (full recomputation)
- ❌ Non-deterministic functions
- ❌ Some complex nested queries

## Complete Example: Sales Analytics

```python
import dlt
from pyspark.sql import functions as F

# Source tables (streaming or batch)
@dlt.table()
def customers():
    return spark.read.table("raw.customers")

@dlt.table()
def orders():
    return spark.read.table("raw.orders")

@dlt.table()
def order_items():
    return spark.read.table("raw.order_items")

@dlt.table()
def products():
    return spark.read.table("raw.products")

# Materialized view: Customer order summary
@dlt.table(
    comment="Customer purchase summary with incremental refresh",
    table_properties={
        "quality": "gold",
        "department": "sales"
    }
)
def customer_order_summary():
    return (
        dlt.read("customers").alias("c")
        .join(dlt.read("orders").alias("o"), "customer_id")
        .join(dlt.read("order_items").alias("oi"), "order_id")
        .join(dlt.read("products").alias("p"), "product_id")
        .groupBy("c.customer_id", "c.customer_name", "c.region")
        .agg(
            F.count("o.order_id").alias("total_orders"),
            F.sum("oi.quantity * oi.unit_price").alias("total_revenue"),
            F.avg("oi.quantity * oi.unit_price").alias("avg_order_value"),
            F.countDistinct("p.product_id").alias("unique_products"),
            F.max("o.order_date").alias("last_order_date")
        )
    )

# Materialized view: Product performance
@dlt.table(
    comment="Product sales performance metrics"
)
def product_performance():
    return (
        dlt.read("order_items").alias("oi")
        .join(dlt.read("products").alias("p"), "product_id")
        .join(dlt.read("orders").alias("o"), "order_id")
        .groupBy("p.product_id", "p.product_name", "p.category")
        .agg(
            F.sum("oi.quantity").alias("total_units_sold"),
            F.sum("oi.quantity * oi.unit_price").alias("total_revenue"),
            F.countDistinct("o.customer_id").alias("unique_customers"),
            F.avg("oi.unit_price").alias("avg_selling_price")
        )
    )

# Materialized view: Daily sales trends
@dlt.table(
    comment="Daily aggregated sales trends"
)
def daily_sales_trends():
    return (
        dlt.read("orders").alias("o")
        .join(dlt.read("order_items").alias("oi"), "order_id")
        .groupBy(F.date_trunc("day", "o.order_date").alias("sale_date"))
        .agg(
            F.count("o.order_id").alias("order_count"),
            F.sum("oi.quantity * oi.unit_price").alias("daily_revenue"),
            F.countDistinct("o.customer_id").alias("active_customers")
        )
        .orderBy("sale_date")
    )
```

## SQL Syntax

```sql
-- Simple materialized view
CREATE OR REFRESH MATERIALIZED VIEW customer_summary AS
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent
FROM orders
GROUP BY customer_id

-- With joins
CREATE OR REFRESH MATERIALIZED VIEW regional_performance AS
SELECT
    r.region_name,
    r.country,
    COUNT(DISTINCT o.order_id) as order_count,
    SUM(o.total_amount) as total_revenue
FROM regions r
INNER JOIN orders o ON r.region_id = o.region_id
GROUP BY r.region_name, r.country

-- Time-based aggregation
CREATE OR REFRESH MATERIALIZED VIEW monthly_metrics AS
SELECT
    DATE_TRUNC('month', order_date) as month,
    product_category,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count,
    AVG(amount) as avg_transaction
FROM sales
GROUP BY DATE_TRUNC('month', order_date), product_category
```

## Incremental Refresh Optimization

### What Triggers Full Recomputation?

**Full refresh required when:**
- ❌ Schema changes in source tables
- ❌ View definition changes
- ❌ Certain aggregation types (window functions)
- ❌ Non-deterministic expressions
- ❌ First pipeline run

**Incremental refresh possible when:**
- ✅ Simple inserts to source tables
- ✅ Updates with proper keys
- ✅ Filters and projections
- ✅ Deterministic aggregations
- ✅ Standard joins

### Optimizing for Incremental Updates

```python
# ✅ GOOD: Supports incremental refresh
@dlt.table()
def optimized_aggregation():
    return (
        dlt.read("events")
        .filter(F.col("event_date") >= "2024-01-01")
        .groupBy("user_id", "event_type")
        .agg(F.count("*").alias("event_count"))
    )

# ❌ AVOID: Requires full recomputation
@dlt.table()
def requires_full_refresh():
    return (
        dlt.read("events")
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("user_id")
                .orderBy(F.desc("event_time"))
            )
        )
        .filter(F.col("rank") == 1)
    )
```

### Partition Pruning

```python
# Enable partition pruning for better incremental performance
@dlt.table(
    partition_cols=["event_date"]
)
def partitioned_aggregation():
    return (
        dlt.read("events")
        .groupBy("event_date", "category")
        .agg(F.sum("amount").alias("total"))
    )
```

## Performance Considerations

### Small Cardinality Aggregations

**Pattern:** Compute incrementally, overwrite completely

```python
@dlt.table()
def campaign_summary():
    """
    Small number of campaigns (~100s)
    Incremental computation, full overwrite
    """
    return (
        dlt.read("events")
        .groupBy("campaign_id")
        .agg(
            F.count("email").alias("num_messages"),
            F.countDistinct("user_id").alias("unique_users")
        )
    )
```

**Characteristics:**
- Few distinct groups
- Fast full computation
- Simple refresh strategy

### Large Cardinality Aggregations

**Pattern:** Compute incrementally, update selectively

```python
@dlt.table()
def user_activity():
    """
    Millions of users
    Incremental computation and updates
    """
    return (
        dlt.read("events")
        .groupBy("user_id")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("value").alias("total_value"),
            F.max("event_time").alias("last_activity")
        )
    )
```

**Characteristics:**
- Many distinct groups
- Expensive full computation
- Selective row updates

### Time Window Aggregations

**Pattern:** Aggregate incrementally, append when window closes

```python
@dlt.table()
def hourly_metrics():
    """
    Time-based windows
    Append completed windows
    """
    return (
        dlt.read("events")
        .groupBy(F.window("timestamp", "1 hour").alias("time"))
        .agg(
            F.count("email").alias("num_messages"),
            F.sum("value").alias("total_value")
        )
    )
```

**Characteristics:**
- Fixed time windows
- Append-only for closed windows
- Update only current window

## Best Practices

### ✅ DO

1. **Design for incremental refresh**
   ```python
   # Use simple aggregations
   .groupBy("key").agg(F.sum("value"))

   # Avoid window functions when possible
   # Use partitioning instead
   ```

2. **Partition large views**
   ```python
   @dlt.table(partition_cols=["date", "region"])
   def partitioned_view():
       return (
           dlt.read("source")
           .groupBy("date", "region", "category")
           .agg(F.sum("amount"))
       )
   ```

3. **Use appropriate data types**
   ```python
   # Efficient aggregations
   .agg(
       F.sum("amount").cast("decimal(18,2)"),
       F.count("*").cast("bigint")
   )
   ```

4. **Monitor refresh performance**
   - Track refresh duration
   - Check incremental vs full refreshes
   - Monitor data volume processed

5. **Cache intermediate results**
   ```python
   # For complex multi-step transformations
   @dlt.view()
   def intermediate_result():
       return expensive_transformation()

   @dlt.table()
   def final_view():
       return dlt.read("intermediate_result").aggregate()
   ```

### ❌ DON'T

1. **Don't use non-deterministic functions**
   ```python
   # ❌ BAD: Requires full refresh
   .withColumn("random_val", F.rand())
   .withColumn("current_ts", F.current_timestamp())

   # ✅ GOOD: Deterministic
   .withColumn("created_date", F.to_date("created_at"))
   ```

2. **Don't overuse window functions**
   ```python
   # ❌ AVOID: Expensive, full recomputation
   .withColumn("rank", F.row_number().over(...))

   # ✅ ALTERNATIVE: Use aggregation
   .groupBy("key").agg(F.max("value"))
   ```

3. **Don't create overly complex views**
   ```python
   # ❌ BAD: Too complex for incremental
   multiple_self_joins().with_many_window_functions()

   # ✅ GOOD: Break into stages
   @dlt.view()
   def stage1():
       return simple_transformation()

   @dlt.table()
   def stage2():
       return dlt.read("stage1").aggregate()
   ```

## Monitoring and Troubleshooting

### Check Refresh Mode

```python
# View pipeline metrics to see if refresh was incremental
# Look for:
# - "Incremental refresh"  vs  "Full refresh"
# - Rows processed
# - Duration
```

### Metrics to Monitor

| Metric | What to Track | Alert Threshold |
|--------|--------------|-----------------|
| **Refresh duration** | Time to update view | > 10 minutes |
| **Rows processed** | Data volume per refresh | Unexpected spikes |
| **Refresh mode** | Incremental vs full | Unexpected full refreshes |
| **View size** | Storage consumed | Rapid growth |
| **Query performance** | Read latency | > 5 seconds |

### Force Full Refresh

```python
# When schema changes or definition updates
# Pipeline automatically detects and does full refresh

# To manually trigger:
# 1. Stop pipeline
# 2. Drop view
# 3. Restart pipeline
```

## Advanced Patterns

### Multi-Layer Materialization

```python
# Layer 1: Base aggregations
@dlt.table()
def daily_user_metrics():
    return (
        dlt.read("events")
        .groupBy("date", "user_id")
        .agg(F.count("*").alias("daily_events"))
    )

# Layer 2: Weekly rollup (uses Layer 1)
@dlt.table()
def weekly_user_metrics():
    return (
        dlt.read("daily_user_metrics")
        .withColumn("week", F.date_trunc("week", "date"))
        .groupBy("week", "user_id")
        .agg(F.sum("daily_events").alias("weekly_events"))
    )

# Layer 3: Monthly rollup (uses Layer 2)
@dlt.table()
def monthly_user_metrics():
    return (
        dlt.read("weekly_user_metrics")
        .withColumn("month", F.date_trunc("month", "week"))
        .groupBy("month", "user_id")
        .agg(F.sum("weekly_events").alias("monthly_events"))
    )
```

### Conditional Materialization

```python
# Only materialize for specific conditions
@dlt.table()
def high_value_customers():
    """Only customers with >$10K lifetime value"""
    return (
        dlt.read("customer_order_summary")
        .filter(F.col("total_revenue") >= 10000)
    )

@dlt.table()
def recent_activity():
    """Only last 90 days of activity"""
    return (
        dlt.read("user_activity")
        .filter(
            F.col("last_activity") >= F.current_date() - F.expr("INTERVAL 90 DAYS")
        )
    )
```

## Limitations

### Cannot Be Used Outside Pipeline

**Materialized views can only be queried within the pipeline that created them.**

**Workaround:**
```python
# Create a streaming table that reads from the materialized view
@dlt.table()
def exportable_summary():
    """Can be queried from outside pipeline"""
    return spark.read.table("materialized_view_name")
```

### Some Operations Require Full Refresh

- Window functions
- Non-deterministic expressions
- Complex self-joins
- Certain nested subqueries

### Latency Considerations

**Not designed for:**
- ❌ Millisecond-level latency
- ❌ Real-time dashboards
- ❌ Instant query results

**Good for:**
- ✅ Scheduled batch updates
- ✅ Daily/hourly refreshes
- ✅ Analytical workloads

## Comparison: Materialized Views vs Streaming Tables

| Feature | Materialized View | Streaming Table |
|---------|------------------|-----------------|
| **Refresh mode** | Batch (triggered) | Continuous/Triggered |
| **Incremental processing** | Automatic (when possible) | Always |
| **Latency** | Minutes to hours | Seconds to minutes |
| **Use case** | Aggregations, reports | Real-time processing |
| **External access** | No | Yes |
| **State management** | Automatic | Manual (watermarks) |
| **Cost** | Lower (batch) | Higher (continuous) |

## Summary

Materialized views in Lakeflow provide:
- ⚡ **Fast query performance** through caching
- 💰 **Cost efficiency** via incremental updates
- ✅ **Correctness guarantees** even with late data
- 🔄 **Automatic refresh** on pipeline updates

**Best for:**
- Aggregation queries
- Join-heavy analytics
- Scheduled reports
- Data marts and summaries

**Avoid for:**
- Real-time dashboards
- External tool access (use streaming tables)
- Complex window operations (unless necessary)
