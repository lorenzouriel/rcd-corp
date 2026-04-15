# Lakeflow Pipelines Limitations

> Source: https://docs.databricks.com/aws/en/dlt/limitations

## Workspace Constraints

### Concurrent Pipeline Updates

**Limit:** 200 concurrent pipeline updates per workspace

**Impact:**
- Shared resource across all pipelines
- Large workspaces may hit limit
- Queued pipelines wait for slot availability

**Mitigation:**
- Schedule pipelines to avoid overlap
- Use triggered mode instead of continuous
- Consider workspace organization

### Dataset Count

**Limit:** Depends on pipeline configuration and workload complexity

**Factors affecting limit:**
- Available compute resources
- Pipeline complexity
- Memory requirements
- Transformation logic

**Mitigation:**
- Break large pipelines into smaller ones
- Optimize transformations
- Increase cluster size
- Use appropriate autoscaling

## Dataset Definition Restrictions

### Single Definition Rule

**Limitation:** Datasets can be defined only once

```python
# ❌ INVALID: Same table defined twice
@dlt.table()
def orders():
    return spark.read.table("raw_orders")

@dlt.table()  # Error: orders already defined
def orders():
    return spark.read.table("backup_orders")

# ✅ VALID: Use different names
@dlt.table()
def orders_primary():
    return spark.read.table("raw_orders")

@dlt.table()
def orders_backup():
    return spark.read.table("backup_orders")
```

### Single Target Operation

**Limitation:** Dataset can be target of only one operation

**Exception:** Streaming tables with append flow processing

```python
# ❌ INVALID: Multiple operations writing to same target
dlt.apply_changes(target="customers", ...)
dlt.apply_changes(target="customers", ...)  # Error

# ✅ VALID: Different targets
dlt.apply_changes(target="customers_current", ...)
dlt.apply_changes(target="customers_history", ...)

# ✅ VALID EXCEPTION: Append flows
dlt.append_flow(
    target="events",
    source="source1",
    name="flow1"
)
dlt.append_flow(
    target="events",  # OK for append flows
    source="source2",
    name="flow2"
)
```

## Identity Column Limitations

### Not Supported with AUTO CDC

**Limitation:** Identity columns not compatible with AUTO CDC processing

```python
# ❌ INVALID: Identity column with AUTO CDC
dlt.create_streaming_table(
    "customers",
    schema="id BIGINT GENERATED ALWAYS AS IDENTITY, ..."
)

dlt.apply_changes(
    target="customers",  # Error: has identity column
    source="cdc_source",
    keys=["customer_id"]
)

# ✅ VALID: No identity column
dlt.create_streaming_table(
    "customers",
    schema="id BIGINT, ..."
)
```

### Recomputation in Materialized Views

**Limitation:** Identity values might be recomputed during materialized view updates

```sql
-- ⚠️ WARNING: Identity values may change on refresh
CREATE OR REFRESH MATERIALIZED VIEW customers
AS SELECT
    row_number() OVER (ORDER BY created_at) as id,  -- May change
    name,
    email
FROM raw_customers
```

**Recommendation:** Only use identity columns for streaming tables

```python
# ✅ RECOMMENDED: Identity column in streaming table
@dlt.table(
    schema="id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING, email STRING"
)
def customers_streaming():
    return spark.readStream.table("raw_customers")
```

## Access and Compatibility Constraints

### Materialized Views and Streaming Tables Access

**Limitation:** Only accessible by Databricks clients

**Accessible from:**
- ✅ Databricks SQL
- ✅ Databricks notebooks
- ✅ Databricks jobs
- ✅ Other Lakeflow pipelines (same workspace)

**Not accessible from:**
- ❌ External BI tools (directly)
- ❌ Third-party query engines
- ❌ Non-Databricks clients

**Workaround:** Use `sink` API for external access

```python
# Export to external system
dlt.create_sink(
    target="kafka_sink",
    source="my_table",
    kafka_bootstrap_servers="broker:9092",
    kafka_topic="output_topic"
)
```

### Unity Catalog Compatibility

**Requirements:**
- Must use Unity Catalog-enabled workspace
- Specific compute access modes
- Cannot use JAR libraries

**Cannot upgrade:** Hive metastore pipelines to Unity Catalog

## Query and Functionality Restrictions

### Delta Lake Time Travel

**Limitation:** Only supported for streaming tables

```sql
-- ✅ VALID: Time travel on streaming table
SELECT * FROM streaming_table
VERSION AS OF 10

SELECT * FROM streaming_table
TIMESTAMP AS OF '2024-01-01'

-- ❌ INVALID: Time travel on materialized view
SELECT * FROM materialized_view
VERSION AS OF 10  -- Error
```

### Iceberg Read Support

**Limitation:** Cannot enable Iceberg reads on materialized views and streaming tables

```python
# ❌ INVALID: Iceberg reads not supported
@dlt.table()
def iceberg_view():
    return (
        spark.read
        .format("iceberg")
        .load("catalog.schema.table")
    )
```

### PIVOT Function

**Limitation:** `pivot()` function is not supported

```sql
-- ❌ INVALID: PIVOT not supported
CREATE OR REFRESH MATERIALIZED VIEW pivoted_sales AS
SELECT *
FROM sales
PIVOT (
    SUM(amount) FOR month IN ('Jan', 'Feb', 'Mar')
)

-- ✅ WORKAROUND: Use CASE statements
CREATE OR REFRESH MATERIALIZED VIEW pivoted_sales AS
SELECT
    product_id,
    SUM(CASE WHEN month = 'Jan' THEN amount ELSE 0 END) as Jan,
    SUM(CASE WHEN month = 'Feb' THEN amount ELSE 0 END) as Feb,
    SUM(CASE WHEN month = 'Mar' THEN amount ELSE 0 END) as Mar
FROM sales
GROUP BY product_id
```

## Compute and Catalog Limitations

### Unity Catalog Pipelines

**Cannot use:**
- Custom JAR libraries
- Schema modifications on streaming tables
- Materialized views outside original pipeline

**Requires:**
- Standard access mode compute
- Specific permission grants
- Unity Catalog-enabled workspace

### Serverless Limitations

**Cannot configure:**
- Worker count
- Instance types
- Cluster size
- Custom Spark configurations

**Requires:**
- Unity Catalog
- Serverless-enabled region
- Accepted terms of use

## Additional Constraints

### Schema Evolution

**Streaming Tables:**
- ✅ Supports schema evolution
- ✅ Can add columns
- ❌ Cannot modify existing columns
- ❌ Cannot drop columns

**Materialized Views:**
- ✅ Full schema flexibility
- ✅ Schema recomputed on refresh

### Cross-Pipeline Dependencies

**Limitation:** Cannot reference tables from other pipelines

```python
# ❌ INVALID: Reference table from other pipeline
@dlt.table()
def combined_data():
    return spark.read.table("other_pipeline.my_table")

# ✅ VALID: Read from Unity Catalog
@dlt.table()
def combined_data():
    return spark.read.table("catalog.schema.table")
```

### Nested Transactions

**Limitation:** DML operations within pipeline code may have restrictions

```python
# ⚠️ CAUTION: Avoid complex DML in transformations
@dlt.table()
def problematic_pattern():
    df = spark.read.table("source")

    # Avoid this pattern
    df.write.mode("overwrite").saveAsTable("temp_table")

    return spark.read.table("temp_table")
```

## Workarounds and Best Practices

### For External Access

```python
# Use sink API
dlt.create_sink(
    target="external_kafka",
    source="my_materialized_view",
    kafka_bootstrap_servers="broker:9092",
    kafka_topic="output"
)

# Or write to regular Delta table
@dlt.table()
def exportable_table():
    return spark.read.table("my_materialized_view")
```

### For PIVOT Operations

```sql
-- Use CASE statements
CREATE OR REFRESH MATERIALIZED VIEW monthly_sales AS
SELECT
    product_id,
    SUM(CASE WHEN month = 1 THEN amount END) as jan_sales,
    SUM(CASE WHEN month = 2 THEN amount END) as feb_sales,
    SUM(CASE WHEN month = 3 THEN amount END) as mar_sales
FROM sales
GROUP BY product_id
```

### For Dataset Reuse

```python
# Create view for reuse
@dlt.view()
def common_transformation():
    return spark.read.table("source").filter("status = 'ACTIVE'")

# Use view in multiple tables
@dlt.table()
def table1():
    return dlt.read("common_transformation").select("col1", "col2")

@dlt.table()
def table2():
    return dlt.read("common_transformation").select("col3", "col4")
```

### For Identity Columns

```python
# Use sequence number instead
from pyspark.sql import functions as F

@dlt.table()
def with_sequence():
    return (
        dlt.read("source")
        .withColumn("seq_id", F.monotonically_increasing_id())
    )
```

## Performance Considerations

### Large Number of Datasets

**Impact on:**
- Pipeline startup time
- Optimization time
- Resource requirements

**Optimization:**
- Combine related transformations
- Use views for intermediate steps
- Split into multiple pipelines

### Complex Transformations

**May hit limits with:**
- Deep nested queries
- Wide dependencies
- Heavy aggregations

**Optimization:**
- Break into stages
- Materialize intermediate results
- Use appropriate caching

## Summary Table

| Feature | Limitation | Workaround |
|---------|-----------|------------|
| **Concurrent updates** | 200 per workspace | Schedule appropriately |
| **Dataset definition** | Once per pipeline | Use different names |
| **Target operations** | One per dataset | Use different targets |
| **Identity columns** | Not with AUTO CDC | Use regular columns |
| **External access** | Databricks only | Use sink API |
| **Time travel** | Streaming tables only | Use streaming tables |
| **PIVOT** | Not supported | Use CASE statements |
| **JAR libraries** | Not in Unity Catalog | Use Python libraries |
| **Schema changes** | Limited on streaming tables | Use materialized views |
| **Cross-pipeline refs** | Not supported | Use Unity Catalog tables |

## Best Practices

### ✅ DO

1. **Plan for limitations** early in design
2. **Use views** for reusable logic
3. **Test limits** in development
4. **Monitor resource usage**
5. **Follow recommended patterns**

### ❌ DON'T

1. **Don't assume** all SQL features work
2. **Don't ignore** concurrent update limits
3. **Don't define** datasets multiple times
4. **Don't use** identity columns with CDC
5. **Don't expect** external tool access

## Getting Help

If you encounter limitations:
1. Review documentation for workarounds
2. Check community forums
3. Contact Databricks support
4. Consider feature requests
