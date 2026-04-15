# SQL Table Syntax for Lakeflow

> Source: https://docs.databricks.com/aws/en/dlt/sql-dev
> Lines: < 200

## Pipeline Creation Syntax

### Streaming Tables

```sql
CREATE OR REFRESH STREAMING TABLE table_name
AS SELECT * FROM source
```

### Materialized Views

```sql
CREATE OR REFRESH MATERIALIZED VIEW view_name
AS SELECT * FROM source
```

## Data Loading with Auto Loader

### Basic Usage (read_files)

```sql
CREATE OR REFRESH STREAMING TABLE orders AS
SELECT *
FROM STREAM read_files(
    "/path/to/data",
    format => "json"
)
```

### Supported Formats

- JSON
- CSV
- Parquet
- Avro
- Delta
- Text

### Read Files with Options

```sql
CREATE OR REFRESH STREAMING TABLE customers AS
SELECT *
FROM STREAM read_files(
    "s3://bucket/customers/",
    format => "json",
    schemaLocation => "/tmp/schema/customers",
    cloudFiles.inferColumnTypes => "true"
)
```

## Data Quality Constraints

### Basic Constraints

```sql
CREATE OR REFRESH STREAMING TABLE orders(
    CONSTRAINT valid_date
    EXPECT (order_datetime IS NOT NULL)
)
AS SELECT * FROM source
```

### Drop Invalid Rows

```sql
CREATE OR REFRESH STREAMING TABLE clean_orders(
    CONSTRAINT valid_id
    EXPECT (order_id IS NOT NULL)
    ON VIOLATION DROP ROW,

    CONSTRAINT positive_amount
    EXPECT (amount > 0)
    ON VIOLATION DROP ROW
)
AS SELECT * FROM raw_orders
```

### Fail on Violation

```sql
CREATE OR REFRESH STREAMING TABLE critical_data(
    CONSTRAINT required_field
    EXPECT (critical_column IS NOT NULL)
    ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM source
```

## Table Properties

```sql
CREATE OR REFRESH STREAMING TABLE customers_bronze
COMMENT "Raw customer data from cloud storage"
TBLPROPERTIES (
    "quality" = "bronze",
    "source_system" = "s3"
)
AS
SELECT * FROM STREAM read_files("s3://bucket/customers/", format => "json")
```

**Reserved Properties (Do NOT Use):**
- `owner` - Use `ALTER TABLE SET OWNER TO` instead
- `location` - Use `LOCATION` clause instead
- `provider` - Use `USING` clause instead
- `external` - Use `CREATE EXTERNAL TABLE` instead

## Medallion Architecture Example

### Bronze Layer

```sql
CREATE OR REFRESH STREAMING TABLE customers_bronze
COMMENT "Raw customer data from cloud storage"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT *
FROM STREAM read_files("s3://bucket/customers/", format => "json")
```

### Silver Layer

```sql
CREATE OR REFRESH STREAMING TABLE customers_silver(
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_email EXPECT (email LIKE '%@%.%') ON VIOLATION DROP ROW
)
COMMENT "Cleaned customer data"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
    customer_id,
    name,
    email,
    CURRENT_TIMESTAMP() as processed_at
FROM STREAM customers_bronze
```

### Gold Layer

```sql
CREATE OR REFRESH MATERIALIZED VIEW customer_domains
COMMENT "Customer email domains summary"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
    SPLIT(email, '@')[1] as domain,
    COUNT(*) as customer_count
FROM customers_silver
GROUP BY SPLIT(email, '@')[1]
```

## Parameterization

### Define Parameters

```sql
SET catalog_name = "production";
SET schema_name = "sales";
SET source_path = "s3://bucket/data/";
```

### Use Parameters

```sql
CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${schema_name}.orders
AS
SELECT *
FROM STREAM read_files("${source_path}orders/", format => "json")
```

## Related

- [SQL Streaming Patterns](sql-streaming.md)
- [Data Quality Expectations](expectations.md)
- [CDC SQL Syntax](cdc-sql.md)
