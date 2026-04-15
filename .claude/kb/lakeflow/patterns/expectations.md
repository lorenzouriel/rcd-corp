# Data Quality Expectations

> Source: https://docs.databricks.com/aws/en/dlt/expectations
> Lines: < 200

## Overview

**Apply quality constraints that validate data as it flows through ETL pipelines**

Expectations are quality checks that ensure data meets business rules and technical requirements before being written to target tables.

## Expectation Components

1. **Expectation name**: Identifier for the rule
2. **Constraint clause**: SQL boolean expression
3. **Action on violation**: What to do with invalid records

## Violation Policies

### 1. WARN (Default)

Invalid records are retained, metrics are tracked

```python
@dlt.expect("valid_age", "age >= 0")
@dlt.table()
def customers():
    return spark.read.table("raw_customers")
```

```sql
CREATE OR REFRESH STREAMING TABLE customers(
    CONSTRAINT valid_age
    EXPECT (age >= 0)
)
AS SELECT * FROM raw_customers
```

**Use Cases:** Exploratory data analysis, understanding data quality issues

### 2. DROP

Invalid records are dropped before data is written to the target

```python
@dlt.expect_or_drop("valid_email", "email LIKE '%@%.%'")
@dlt.table()
def customers():
    return spark.read.table("raw_customers")
```

```sql
CREATE OR REFRESH STREAMING TABLE customers(
    CONSTRAINT valid_email
    EXPECT (email LIKE '%@%.%')
    ON VIOLATION DROP ROW
)
AS SELECT * FROM raw_customers
```

**Use Cases:** Production pipelines, strict data quality requirements

### 3. FAIL

Invalid records prevent the update from succeeding

```python
@dlt.expect_or_fail("critical_field", "customer_id IS NOT NULL")
@dlt.table()
def customers():
    return spark.read.table("raw_customers")
```

```sql
CREATE OR REFRESH STREAMING TABLE customers(
    CONSTRAINT critical_field
    EXPECT (customer_id IS NOT NULL)
    ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM raw_customers
```

**Use Cases:** Critical business data, compliance requirements

## Constraint Types

### Simple Comparisons

```python
@dlt.expect("positive_amount", "amount > 0")
@dlt.expect("valid_status", "status IN ('ACTIVE', 'INACTIVE', 'PENDING')")
@dlt.expect("recent_date", "transaction_date >= '2020-01-01'")
```

### SQL Functions

```python
@dlt.expect("valid_year", "YEAR(transaction_date) >= 2020")
@dlt.expect("valid_length", "LENGTH(customer_name) >= 3")
@dlt.expect("uppercase_code", "country_code = UPPER(country_code)")
```

### CASE Statements

```python
@dlt.expect(
    "conditional_validation",
    """
    CASE
        WHEN type = 'RETAIL' THEN amount <= 10000
        WHEN type = 'WHOLESALE' THEN amount <= 100000
        ELSE TRUE
    END
    """
)
```

### Complex Boolean Logic

```python
@dlt.expect(
    "complex_rule",
    "(status = 'ACTIVE' AND last_login IS NOT NULL) OR status = 'INACTIVE'"
)
```

## Multiple Expectations

### Python: Individual Decorators

```python
@dlt.expect_or_drop("valid_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email LIKE '%@%.%'")
@dlt.expect_or_drop("positive_age", "age > 0 AND age < 150")
@dlt.table()
def customers():
    return spark.read.table("raw_customers")
```

### SQL: Multiple Constraints

```sql
CREATE OR REFRESH STREAMING TABLE customers(
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_email EXPECT (email LIKE '%@%.%') ON VIOLATION DROP ROW,
    CONSTRAINT positive_age EXPECT (age > 0 AND age < 150) ON VIOLATION DROP ROW
)
AS SELECT * FROM raw_customers
```

## Related

- [Advanced Expectations](expectations-advanced.md)
- [CDC Patterns](cdc-apply-changes.md)
- [Python Decorators](python-decorators.md)
