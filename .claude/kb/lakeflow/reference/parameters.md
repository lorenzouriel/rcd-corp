# Pipeline Parameters

> Source: https://docs.databricks.com/aws/en/dlt/parameters

## Overview

Pipeline parameters are key-value configuration pairs that allow you to:
- Separate long paths from code
- Reduce data processing in development
- Reuse transformation logic across data sources
- Control data sources across environments

## Parameter Definition

### Key Requirements

✅ **Allowed characters:** Alphanumeric, `_`, `-`, `.`
❌ **Not allowed:** Special characters, spaces
⚠️ **Values:** Always set as strings
⚠️ **Dynamic values:** Not supported

### Valid Parameter Names

```
✅ source_catalog
✅ my_parameter_123
✅ pipeline.start-date
✅ env.production_path

❌ source catalog  (space)
❌ param@value    (special char)
❌ 123param       (starts with number)
```

## Setting Parameters

### UI Configuration

1. Navigate to pipeline settings
2. Go to "Configuration" section
3. Add key-value pairs

### JSON Configuration

```json
{
  "configuration": {
    "source_catalog": "production",
    "source_schema": "sales",
    "source_path": "s3://bucket/data/",
    "lookback_days": "7",
    "mypipeline.startDate": "2021-01-02"
  }
}
```

### Environment-Specific Parameters

```json
{
  "configuration": {
    "env": "production",
    "catalog": "prod_catalog",
    "schema": "prod_schema",
    "source_bucket": "s3://prod-data/",
    "notifications_email": "data-team@company.com"
  }
}
```

## Using Parameters

### Python

#### Access Configuration

```python
import dlt

# Get parameter value
source_catalog = spark.conf.get("source_catalog")
source_schema = spark.conf.get("schema_name")
source_path = spark.conf.get("source_path")
```

#### Use in Table Definitions

```python
@dlt.table()
def sales_data():
    catalog = spark.conf.get("source_catalog")
    schema = spark.conf.get("source_schema")
    return spark.read.table(f"{catalog}.{schema}.raw_sales")
```

#### Use in File Paths

```python
@dlt.table()
def cloud_data():
    path = spark.conf.get("source_path")
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(path)
    )
```

#### Complete Python Example

```python
import dlt
from pyspark.sql import functions as F

# Get parameters
CATALOG = spark.conf.get("source_catalog")
SCHEMA = spark.conf.get("source_schema")
PATH = spark.conf.get("source_path")
LOOKBACK_DAYS = int(spark.conf.get("lookback_days", "7"))

# Bronze: Parameterized ingestion
@dlt.table()
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{PATH}orders/")
    )

# Silver: Use parameter in transformation
@dlt.table()
def orders_silver():
    cutoff_date = F.current_date() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS")
    return (
        dlt.read_stream("orders_bronze")
        .filter(F.col("order_date") >= cutoff_date)
    )

# Gold: Cross-catalog join
@dlt.table()
def orders_with_customers():
    return (
        spark.read.table("orders_silver")
        .join(
            spark.read.table(f"{CATALOG}.{SCHEMA}.customers"),
            "customer_id"
        )
    )
```

### SQL

#### String Interpolation

```sql
-- Use ${parameter_name} for string interpolation
CREATE OR REFRESH STREAMING TABLE sales_data
AS SELECT *
FROM ${source_catalog}.${source_schema}.raw_sales
```

#### Complete SQL Example

```sql
-- Configuration:
-- {
--   "source_catalog": "production",
--   "source_schema": "sales",
--   "source_path": "s3://bucket/data/"
-- }

-- Bronze: Parameterized ingestion
CREATE OR REFRESH STREAMING TABLE orders_bronze
AS SELECT *
FROM STREAM read_files(
    "${source_path}orders/",
    format => "json"
)

-- Silver: Transform
CREATE OR REFRESH STREAMING TABLE orders_silver(
    CONSTRAINT valid_order EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT * FROM STREAM orders_bronze

-- Gold: Cross-catalog join
CREATE OR REFRESH MATERIALIZED VIEW orders_with_customers
AS SELECT
    o.*,
    c.customer_name,
    c.customer_email
FROM orders_silver o
INNER JOIN ${source_catalog}.${source_schema}.customers c
ON o.customer_id = c.customer_id
```

## Common Use Cases

### 1. Environment-Specific Paths

**Problem:** Different data paths for dev/staging/prod

**Solution:**
```json
{
  "configuration": {
    "env": "production",
    "data_path": "s3://prod-data-bucket/",
    "checkpoint_path": "s3://prod-checkpoints/"
  }
}
```

```python
env = spark.conf.get("env")
data_path = spark.conf.get("data_path")

@dlt.table()
def ingestion():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}schema/")
        .load(data_path)
    )
```

### 2. Reduce Data in Development

**Problem:** Full data processing too slow in development

**Solution:**
```json
{
  "configuration": {
    "limit_rows": "1000",
    "sample_fraction": "0.01"
  }
}
```

```python
limit_rows = int(spark.conf.get("limit_rows", "0"))

@dlt.table()
def dev_sample():
    df = spark.read.table("production.sales.raw_data")
    if limit_rows > 0:
        df = df.limit(limit_rows)
    return df
```

### 3. Reuse Logic Across Sources

**Problem:** Same transformation for multiple data sources

**Solution:**
```json
{
  "configuration": {
    "source_type": "kafka",
    "kafka_topic": "orders",
    "kafka_servers": "broker1:9092"
  }
}
```

```python
source_type = spark.conf.get("source_type")
topic = spark.conf.get("kafka_topic")
servers = spark.conf.get("kafka_servers")

@dlt.table()
def generic_ingestion():
    if source_type == "kafka":
        return (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", servers)
            .option("subscribe", topic)
            .load()
        )
    else:
        path = spark.conf.get("file_path")
        return spark.readStream.load(path)
```

### 4. Date Range Processing

**Problem:** Process historical data with configurable date ranges

**Solution:**
```json
{
  "configuration": {
    "start_date": "2024-01-01",
    "end_date": "2024-12-31"
  }
}
```

```python
start_date = spark.conf.get("start_date")
end_date = spark.conf.get("end_date")

@dlt.table()
def historical_data():
    return (
        spark.read.table("raw_events")
        .filter(F.col("event_date").between(start_date, end_date))
    )
```

## Best Practices

### ✅ DO

1. **Use descriptive names**
   ```
   ✅ source_catalog
   ✅ kafka_bootstrap_servers
   ❌ sc
   ❌ param1
   ```

2. **Follow naming conventions**
   ```
   snake_case for Python
   dot.notation.for.hierarchical.configs
   ```

3. **Provide defaults**
   ```python
   lookback_days = int(spark.conf.get("lookback_days", "7"))
   ```

4. **Document parameters**
   ```json
   {
     "configuration": {
       "source_path": "s3://bucket/data/",  // Source data location
       "lookback_days": "7"                  // Days to look back for incremental processing
     }
   }
   ```

5. **Validate parameter values**
   ```python
   env = spark.conf.get("env")
   assert env in ["dev", "staging", "production"], f"Invalid env: {env}"
   ```

6. **Use environment-specific config files**
   ```
   config/dev.json
   config/staging.json
   config/production.json
   ```

### ❌ DON'T

1. **Don't use reserved Spark keywords**
   ```
   ❌ spark.executor.memory
   ❌ spark.sql.shuffle.partitions
   ✅ custom.executor.memory
   ```

2. **Don't use dynamic values**
   ```
   ❌ "current_date": "${current_date()}"
   ✅ Set via pipeline update
   ```

3. **Don't hardcode sensitive data**
   ```
   ❌ "api_key": "secret123"
   ✅ Use Databricks Secrets
   ```

4. **Don't use complex types**
   ```
   ❌ Parameters are always strings
   ✅ Convert in code: int(spark.conf.get("limit"))
   ```

## Advanced Patterns

### Multi-Environment Configuration

```python
import dlt
from pyspark.sql import functions as F

ENV = spark.conf.get("env", "dev")

CONFIG = {
    "dev": {
        "catalog": "dev_catalog",
        "limit": 1000,
        "sample": True
    },
    "staging": {
        "catalog": "staging_catalog",
        "limit": 10000,
        "sample": False
    },
    "production": {
        "catalog": "prod_catalog",
        "limit": None,
        "sample": False
    }
}

env_config = CONFIG[ENV]

@dlt.table()
def orders():
    df = spark.read.table(f"{env_config['catalog']}.sales.raw_orders")

    if env_config['sample']:
        df = df.sample(0.01)

    if env_config['limit']:
        df = df.limit(env_config['limit'])

    return df
```

### Feature Flags

```python
enable_quality_checks = spark.conf.get("enable_quality_checks", "true").lower() == "true"
enable_pii_masking = spark.conf.get("enable_pii_masking", "false").lower() == "true"

@dlt.table()
def customers():
    df = spark.read.table("raw_customers")

    if enable_pii_masking:
        df = df.withColumn("ssn", F.expr("mask(ssn)"))

    return df

if enable_quality_checks:
    @dlt.expect_or_drop("valid_email", "email LIKE '%@%.%'")
```

### Dynamic Table Names

```python
table_prefix = spark.conf.get("table_prefix", "")

@dlt.table(name=f"{table_prefix}orders")
def orders():
    return spark.read.table("raw_orders")

@dlt.table(name=f"{table_prefix}customers")
def customers():
    return spark.read.table("raw_customers")
```

## Troubleshooting

### Parameter Not Found

```python
# DON'T: Fails if parameter doesn't exist
value = spark.conf.get("my_param")

# DO: Provide default value
value = spark.conf.get("my_param", "default_value")
```

### Type Conversion Errors

```python
# DON'T: Assumes integer
limit = spark.conf.get("limit")
df.limit(limit)  # TypeError

# DO: Convert string to int
limit = int(spark.conf.get("limit", "100"))
df.limit(limit)
```

### Reserved Keywords Conflict

```python
# DON'T: Conflicts with Spark config
spark.conf.get("spark.executor.cores")

# DO: Use custom prefix
spark.conf.get("pipeline.executor.cores")
```

## Security Considerations

### Don't Store Secrets in Parameters

```python
# ❌ BAD: Secrets visible in UI
api_key = spark.conf.get("api_key")

# ✅ GOOD: Use Databricks Secrets
api_key = dbutils.secrets.get(scope="production", key="api_key")
```

### Access Control

- Parameters visible to anyone with pipeline access
- Use secrets for sensitive values
- Limit pipeline permissions appropriately

## Complete Example: Multi-Environment Pipeline

```json
// Development config
{
  "configuration": {
    "env": "dev",
    "catalog": "dev_catalog",
    "schema": "sales_dev",
    "source_path": "s3://dev-bucket/data/",
    "limit_rows": "1000",
    "enable_quality_checks": "false"
  }
}

// Production config
{
  "configuration": {
    "env": "production",
    "catalog": "prod_catalog",
    "schema": "sales",
    "source_path": "s3://prod-bucket/data/",
    "limit_rows": "0",
    "enable_quality_checks": "true"
  }
}
```

```python
import dlt
from pyspark.sql import functions as F

# Read parameters
ENV = spark.conf.get("env")
CATALOG = spark.conf.get("catalog")
SCHEMA = spark.conf.get("schema")
SOURCE_PATH = spark.conf.get("source_path")
LIMIT_ROWS = int(spark.conf.get("limit_rows", "0"))
ENABLE_QC = spark.conf.get("enable_quality_checks", "true").lower() == "true"

# Bronze
@dlt.table()
def orders_bronze():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{SOURCE_PATH}orders/")
    )

    # Limit for dev
    if LIMIT_ROWS > 0:
        df = df.limit(LIMIT_ROWS)

    return df

# Silver with conditional quality checks
if ENABLE_QC:
    @dlt.expect_or_drop("valid_order", "order_id IS NOT NULL")
    @dlt.table()
    def orders_silver():
        return dlt.read_stream("orders_bronze")
else:
    @dlt.table()
    def orders_silver():
        return dlt.read_stream("orders_bronze")

# Gold
@dlt.table()
def daily_orders():
    return (
        spark.read.table("orders_silver")
        .groupBy(F.date_trunc("day", "order_date").alias("date"))
        .count()
    )
```
