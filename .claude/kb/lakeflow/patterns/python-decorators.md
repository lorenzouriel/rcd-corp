# Python Decorators for Lakeflow

> Source: https://docs.databricks.com/aws/en/dlt/python-dev
> Lines: < 150

## Module Import

```python
import dlt
```

**All Lakeflow Declarative Pipelines Python APIs are implemented in the `dlt` module**

## Core Decorators

### @dlt.table()
Creates materialized views or streaming tables

```python
@dlt.table()
def my_table():
    return spark.read.format("json").load("/path/to/data")
```

### @dlt.view()
Creates temporary views for data transformation

```python
@dlt.view()
def my_temp_view():
    return spark.sql("SELECT * FROM source_table")
```

### @dlt.expect_or_drop()
Validates data quality by setting constraints

```python
@dlt.expect_or_drop("valid_data", "column IS NOT NULL")
@dlt.table()
def clean_data():
    return spark.read.table("raw_data")
```

## Table Configuration

### Explicit Table Name

```python
@dlt.table(name="custom_table_name")
def my_function():
    return spark.read.table("source")
```

### Table Properties

```python
@dlt.table(
    comment="Raw customer data from cloud storage",
    table_properties={"quality": "bronze"}
)
def customers_bronze():
    return spark.read.table("source")
```

### Inferred Table Name

```python
@dlt.table()
def orders():  # Table name will be "orders"
    return spark.read.table("raw_orders")
```

## Key Development Patterns

### Functions Must Return DataFrames

```python
@dlt.table()
def my_table():
    # CORRECT: Returns a DataFrame
    return spark.read.table("source")

    # INCORRECT: Does not return a DataFrame
    # df = spark.read.table("source")
```

### Lazy Execution Model

```python
# INCORRECT: This won't work as expected
@dlt.table()
def wrong_pattern():
    df = spark.read.table("source")
    count = df.count()  # Triggers execution too early
    return df

# CORRECT: Keep operations lazy
@dlt.table()
def correct_pattern():
    return spark.read.table("source")
```

### Programmatic Table Creation

```python
tables = ["orders", "customers", "products"]

for table_name in tables:
    @dlt.table(name=f"{table_name}_processed")
    def process_table():
        return spark.read.table(table_name)
```

## Best Practices

### DO

1. **Avoid side effects** in dataset definition functions
2. **Ensure additive dataset definitions** in pipelines
3. **Use expectations** for data quality validation
4. **Return DataFrames** from all decorated functions
5. **Use Auto Loader** for incremental ingestion

### DON'T

1. **Don't trigger actions** (`.count()`, `.collect()`) in pipeline code
2. **Don't use non-deterministic** operations without sequence columns
3. **Don't modify external state** in table definitions
4. **Don't mix batch and streaming** without understanding semantics

## Related

- [Python Streaming Patterns](python-streaming.md)
- [Data Quality Expectations](expectations.md)
- [CDC Patterns](cdc-apply-changes.md)
