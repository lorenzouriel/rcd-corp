# Lakeflow Pipelines ETL Tutorial

> Source: https://docs.databricks.com/aws/en/dlt/tutorial-pipelines

## Tutorial Overview

Create an end-to-end ETL pipeline using Change Data Capture (CDC) that extracts, transforms, and loads customer data.

## Pipeline Architecture

### Medallion Architecture Layers

1. **Bronze Layer**: Raw data ingestion
2. **Silver Layer**: Data cleaning and quality
3. **Gold Layer**: Business-ready tables

## Implementation Steps

### 1. Data Generation
- Use Faker library to create sample CDC data
- Generate fake customer records with operations like APPEND, UPDATE, DELETE

### 2. Bronze Layer: Raw Data Ingestion

```python
@dlt.table()
def customers_cdc_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/path/to/cdc/data")
    )
```

**Features:**
- Use Auto Loader to incrementally ingest JSON files
- Automatically infer schema
- Handle file processing metadata

### 3. Silver Layer: Data Cleaning

```python
@dlt.table()
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "operation IN ('APPEND', 'UPDATE', 'DELETE')")
def customers_cdc_silver():
    return dlt.read_stream("customers_cdc_bronze")
```

**Data Quality Expectations:**
- Drop rows with null IDs
- Drop rows with invalid operations
- Drop rows with rescued data issues

### 4. Gold Layer: Table Materialization

```python
dlt.create_streaming_table("customers")

dlt.apply_changes(
    target="customers",
    source="customers_cdc_silver",
    keys=["id"],
    sequence_by="timestamp",
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "timestamp"]
)
```

**AUTO CDC Features:**
- Process upserts and deletions
- Handle out-of-order events
- Automatic deduplication by sequence

### 5. Historical Tracking (SCD Type 2)

```python
dlt.create_streaming_table("customers_history")

dlt.apply_changes(
    target="customers_history",
    source="customers_cdc_silver",
    keys=["id"],
    sequence_by="timestamp",
    stored_as_scd_type="2"
)
```

**Historical Features:**
- Track all changes over time
- Maintain valid_from and valid_to timestamps
- Enable time-travel queries

### 6. Analytics Layer

```sql
CREATE MATERIALIZED VIEW customer_change_frequency AS
SELECT
    id,
    COUNT(*) as change_count
FROM customers_history
GROUP BY id
```

## Pipeline Features

✅ **Schema evolution** - Automatically handles schema changes
✅ **Out-of-order events** - Processes events by sequence number
✅ **Data quality checks** - Built-in expectations framework
✅ **Incremental processing** - Only processes new data

## Deployment

### Create Scheduled Job

```python
# Configure pipeline to run daily
# Set update mode to "triggered" or "continuous"
# Add notifications for failures
```

## Best Practices

1. **Bronze**: Keep raw data exactly as received
2. **Silver**: Apply quality rules and standardization
3. **Gold**: Create business-specific aggregations
4. **CDC**: Always use sequence columns for ordering
5. **Testing**: Validate expectations before production
