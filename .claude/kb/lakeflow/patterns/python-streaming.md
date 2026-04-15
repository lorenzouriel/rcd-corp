# Python Streaming Patterns for Lakeflow

> Source: https://docs.databricks.com/aws/en/dlt/python-dev
> Lines: < 150

## Data Loading Methods

### Batch Loading

```python
@dlt.table()
def batch_table():
    return spark.read.format("json").load("/path/to/data")
```

### Streaming Loading

```python
@dlt.table()
def streaming_table():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/path/to/data")
```

### Auto Loader (Recommended)

```python
@dlt.table()
def incremental_load():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/path/to/schema")
        .load("s3://bucket/path/")
    )
```

## Complete Medallion Example

```python
import dlt
from pyspark.sql import functions as F

# Bronze: Raw data ingestion
@dlt.table(
    comment="Raw customer data from cloud storage",
    table_properties={"quality": "bronze"}
)
def customers_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/tmp/schema/customers")
        .load("s3://bucket/customers/")
    )

# Silver: Data cleaning
@dlt.table(
    comment="Cleaned customer data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email LIKE '%@%.%'")
def customers_silver():
    return (
        dlt.read_stream("customers_bronze")
        .select(
            "customer_id",
            "name",
            "email",
            F.current_timestamp().alias("processed_at")
        )
    )

# Gold: Business aggregations
@dlt.table(
    comment="Customer email domains summary",
    table_properties={"quality": "gold"}
)
def customer_domains():
    return (
        spark.read.table("customers_silver")
        .withColumn("domain", F.split(F.col("email"), "@")[1])
        .groupBy("domain")
        .count()
    )
```

## Advanced Features

### Change Data Capture (CDC)

```python
dlt.create_streaming_table("target_table")

dlt.apply_changes(
    target="target_table",
    source="source_cdc_stream",
    keys=["id"],
    sequence_by="timestamp",
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "timestamp"]
)
```

### Parameterization

```python
catalog = spark.conf.get("catalog_name")
schema = spark.conf.get("schema_name")

@dlt.table()
def parameterized_table():
    return spark.read.table(f"{catalog}.{schema}.source_table")
```

## Streaming vs Batch Decision

| Use Case | Method | Example |
|----------|--------|---------|
| Real-time ingestion | `readStream` | New files arriving continuously |
| Historical load | `read` | One-time bulk import |
| Incremental updates | `cloudFiles` | Daily file drops |
| Event streams | `readStream` | Kafka, Event Hubs |

## Related

- [Python Decorators](python-decorators.md)
- [CDC Apply Changes](cdc-apply-changes.md)
- [Data Quality Expectations](expectations.md)
