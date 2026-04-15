# Unity Catalog Integration

> Source: https://docs.databricks.com/aws/en/dlt/unity-catalog

## Overview

Unity Catalog provides centralized governance, security, and discovery for all data assets in Lakeflow Declarative Pipelines.

## Configuration

### Enable Unity Catalog

When creating a pipeline:
1. Select **"Unity Catalog"** under storage options
2. Choose a **catalog** for table storage
3. Choose a **schema** within the catalog
4. Ensure compute uses **standard access mode**

### Pipeline Setting

```json
{
  "catalog": "production",
  "target": "production.sales_schema",
  "unity_catalog_enabled": true
}
```

### ⚠️ IMPORTANT: Catalog and Schema Configuration

**The `@dlt.table()` decorator does NOT support `catalog` or `schema` parameters.**

❌ **INCORRECT - This will cause TypeError:**
```python
@dlt.table(
    name="silver_users",
    catalog="semana",  # NOT SUPPORTED
    schema="default"    # NOT SUPPORTED
)
def silver_users():
    return dlt.read_stream("semana.default.bronze_users")
```

✅ **CORRECT - Specify catalog/schema at pipeline configuration level:**
```python
@dlt.table(
    name="silver_users"
)
def silver_users():
    # Read from fully qualified table name
    return dlt.read_stream("semana.default.bronze_users")
```

**Pipeline Configuration (UI or JSON):**
- Set `catalog = "semana"`
- Set `target = "semana.default"`
- All tables created by the pipeline will be written to `semana.default.*`

**Key Points:**
- Use fully qualified names when reading: `catalog.schema.table`
- Table output location is controlled by pipeline configuration, NOT decorator parameters
- All tables in a single pipeline write to the same catalog.schema (unless using Direct Publishing Mode)

## Permission Requirements

### Catalog-Level Permissions

```sql
-- Required for all pipelines
GRANT USE CATALOG ON CATALOG production TO `service_principal`;
```

### Schema-Level Permissions for Materialized Views

```sql
-- Create materialized views
GRANT CREATE MATERIALIZED VIEW ON SCHEMA production.sales TO `service_principal`;
GRANT USE SCHEMA ON SCHEMA production.sales TO `service_principal`;
```

### Schema-Level Permissions for Streaming Tables

```sql
-- Create streaming tables
GRANT CREATE TABLE ON SCHEMA production.sales TO `service_principal`;
GRANT USE SCHEMA ON SCHEMA production.sales TO `service_principal`;
```

### Modifying Existing Tables

```sql
-- Modify tables and views
GRANT MODIFY ON TABLE production.sales.customers TO `service_principal`;
```

## Data Ingestion Capabilities

### Read from Unity Catalog Tables

```python
@dlt.table()
def transformed_data():
    return spark.read.table("catalog.schema.source_table")
```

```sql
CREATE OR REFRESH MATERIALIZED VIEW transformed_data
AS SELECT * FROM catalog.schema.source_table
```

### Read from Hive Metastore Tables

```python
@dlt.table()
def from_hive():
    return spark.read.table("hive_metastore.default.legacy_table")
```

### Auto Loader

```python
@dlt.table()
def cloud_data():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://bucket/data/")
    )
```

### Stream Changes from Tables

```python
@dlt.table()
def incremental_changes():
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table("catalog.schema.source_table")
    )
```

### Apache Kafka

```python
@dlt.table()
def kafka_stream():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "topic")
        .load()
    )
```

### Amazon Kinesis

```python
@dlt.table()
def kinesis_stream():
    return (
        spark.readStream
        .format("kinesis")
        .option("streamName", "my-stream")
        .option("region", "us-west-2")
        .load()
    )
```

## Governance Features

### Lineage Tracking

Unity Catalog **Catalog Explorer** shows:
- **Upstream dependencies** - What data sources feed into tables
- **Downstream consumers** - What queries/dashboards use tables
- **Column-level lineage** - How columns transform through pipeline
- **Cross-pipeline lineage** - Dependencies across pipelines

**Access lineage:**
1. Open Catalog Explorer
2. Navigate to table
3. Click "Lineage" tab

### Access Control

#### Grant Table Access

```sql
-- Grant SELECT to analysts
GRANT SELECT ON TABLE production.sales.customers TO `analysts_group`;

-- Grant ALL to data engineers
GRANT ALL PRIVILEGES ON TABLE production.sales.customers TO `data_engineers`;
```

#### Revoke Access

```sql
REVOKE SELECT ON TABLE production.sales.customers FROM `analysts_group`;
```

### Row Filters (Public Preview)

Restrict row-level access based on user identity:

```sql
CREATE FUNCTION sales_filter()
RETURN IF(IS_MEMBER('sales_team'), TRUE, region = current_user());

ALTER TABLE production.sales.transactions
SET ROW FILTER sales_filter ON (region);
```

### Column Masks (Public Preview)

Mask sensitive data based on user permissions:

```sql
CREATE FUNCTION mask_ssn(ssn STRING)
RETURN CASE
  WHEN IS_MEMBER('admin') THEN ssn
  ELSE 'XXX-XX-' || RIGHT(ssn, 4)
END;

ALTER TABLE production.sales.customers
ALTER COLUMN ssn SET MASK mask_ssn;
```

## DML Statements on Streaming Tables

Unity Catalog pipelines support DML operations on streaming tables:

### INSERT

```sql
INSERT INTO production.sales.customers
VALUES ('123', 'John Doe', 'john@example.com');
```

### UPDATE

```sql
UPDATE production.sales.customers
SET email = 'newemail@example.com'
WHERE customer_id = '123';
```

### DELETE

```sql
DELETE FROM production.sales.customers
WHERE customer_id = '123';
```

### MERGE

```sql
MERGE INTO production.sales.customers target
USING updates source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

**Note:** DML statements on streaming tables are supported only in Unity Catalog pipelines.

## Key Limitations

### ❌ Not Supported

1. **JAR Libraries**
   - Cannot use custom JARs in Unity Catalog pipelines
   - Use Python libraries instead

2. **Schema Modifications on Streaming Tables**
   - Cannot alter schema of streaming tables
   - Recreate table if schema change needed

3. **Materialized Views Outside Pipeline**
   - Cannot query materialized views from outside original pipeline
   - Use streaming tables for external access

4. **Hive Metastore Pipeline Upgrades**
   - Cannot upgrade existing Hive metastore pipelines to Unity Catalog
   - Must create new Unity Catalog pipeline

## Advanced Features

### Inactive Table Recovery

**Retention:** Inactive tables can be recovered within **7 days**

```sql
-- Restore deleted table
UNDROP TABLE production.sales.customers;
```

### Pipeline Deletion Behavior

**Warning:** Pipeline deletion removes all associated tables

**To preserve tables:**
1. Export table definitions
2. Recreate tables outside pipeline
3. Copy data before deletion

### Cross-Catalog References

```python
@dlt.table()
def multi_catalog_join():
    return (
        spark.read.table("catalog1.schema.table1")
        .join(
            spark.read.table("catalog2.schema.table2"),
            "id"
        )
    )
```

## Complete Example: Unity Catalog Pipeline

### Python

```python
import dlt
from pyspark.sql import functions as F

# Read from Unity Catalog
@dlt.table()
def bronze_sales():
    return spark.read.table("production.raw.sales_events")

# Transform with governance
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.table(
    comment="Cleaned sales data with quality checks",
    table_properties={
        "quality": "silver",
        "owner": "data_team",
        "pii": "false"
    }
)
def silver_sales():
    return (
        dlt.read("bronze_sales")
        .select(
            "transaction_id",
            "customer_id",
            "amount",
            F.current_timestamp().alias("processed_at")
        )
    )

# Aggregate for analytics
@dlt.table(
    comment="Daily sales aggregations",
    table_properties={
        "quality": "gold",
        "refresh_schedule": "daily"
    }
)
def gold_daily_sales():
    return (
        spark.read.table("silver_sales")
        .groupBy(F.date_trunc("day", "processed_at").alias("date"))
        .agg(F.sum("amount").alias("total_sales"))
    )
```

### SQL

```sql
-- Read from Unity Catalog
CREATE OR REFRESH STREAMING TABLE bronze_sales
AS SELECT * FROM production.raw.sales_events

-- Transform with governance
CREATE OR REFRESH STREAMING TABLE silver_sales(
    CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
)
COMMENT "Cleaned sales data with quality checks"
TBLPROPERTIES (
    "quality" = "silver",
    "owner" = "data_team",
    "pii" = "false"
)
AS SELECT
    transaction_id,
    customer_id,
    amount,
    CURRENT_TIMESTAMP() as processed_at
FROM STREAM bronze_sales

-- Aggregate for analytics
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales
COMMENT "Daily sales aggregations"
TBLPROPERTIES (
    "quality" = "gold",
    "refresh_schedule" = "daily"
)
AS SELECT
    DATE_TRUNC('day', processed_at) as date,
    SUM(amount) as total_sales
FROM silver_sales
GROUP BY DATE_TRUNC('day', processed_at)
```

## Best Practices

### ✅ DO

1. **Use Unity Catalog by default** for all new pipelines
2. **Configure service principals** for pipeline run-as identity
3. **Apply least privilege** permissions
4. **Document tables** with comments and properties
5. **Use row filters and column masks** for sensitive data
6. **Leverage lineage** for impact analysis
7. **Tag tables** with metadata (owner, quality, PII, etc.)
8. **Monitor access** with audit logs

### ❌ DON'T

1. **Don't grant excessive permissions**
2. **Don't skip documentation**
3. **Don't ignore lineage** information
4. **Don't hardcode catalog names** (use parameters)
5. **Don't delete pipelines** without backing up data
6. **Don't use user accounts** for run-as (use service principals)

## Troubleshooting

### Permission Denied Errors

**Check:**
- `USE CATALOG` on target catalog
- `USE SCHEMA` on target schema
- `CREATE TABLE` or `CREATE MATERIALIZED VIEW` on schema
- `MODIFY` on existing tables

**Fix:**
```sql
GRANT USE CATALOG ON CATALOG production TO `pipeline_sp`;
GRANT USE SCHEMA ON SCHEMA production.sales TO `pipeline_sp`;
GRANT CREATE TABLE ON SCHEMA production.sales TO `pipeline_sp`;
```

### Lineage Not Showing

**Possible causes:**
- Pipeline not run yet
- Lineage computation in progress
- Cross-workspace references

**Fix:**
- Wait for pipeline completion
- Refresh Catalog Explorer
- Check cross-workspace permissions

### Cannot Access from External Tools

**Issue:** Materialized views not accessible outside pipeline

**Solution:**
- Use streaming tables instead
- Copy data to regular Delta tables
- Use `sink` API for external access

## Security Recommendations

1. **Use service principals** for pipeline execution
2. **Enable audit logging** for compliance
3. **Apply row-level security** for sensitive data
4. **Mask PII columns** with column masks
5. **Review permissions** regularly
6. **Monitor access patterns** in audit logs
7. **Use separate catalogs** for dev/staging/production
8. **Encrypt data** at rest and in transit
