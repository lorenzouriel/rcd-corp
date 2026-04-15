# Pipeline Configuration

> Source: https://docs.databricks.com/aws/en/dlt/configure-pipeline

## Default Configuration

When creating a new pipeline, Databricks sets:
- ✅ Unity Catalog enabled
- ✅ Current runtime channel
- ✅ Serverless compute
- ✅ Development mode: OFF

## Compute Configuration

### Recommended: Enhanced Autoscaling

Automatically scales compute resources based on workload.

### Configurable Settings

| Setting | Description |
|---------|-------------|
| **Cluster Policy** | Apply organizational policies |
| **Cluster Mode** | Fixed size or Legacy autoscaling |
| **Min/Max Workers** | Autoscaling boundaries |
| **Photon Acceleration** | Enable for better performance |
| **Instance Profile** | IAM role for AWS resources |
| **Cluster Tags** | For cost tracking and organization |
| **Instance Types** | Worker and driver node types |

### Serverless vs Classic Compute

| Feature | Serverless | Classic |
|---------|-----------|---------|
| **Setup** | Zero configuration | Manual cluster config |
| **Scaling** | Automatic vertical/horizontal | Manual configuration |
| **Startup** | 4-6 minutes | Variable |
| **Management** | Fully managed | Self-managed |
| **Cost** | Pay per use | Pay for cluster time |

## Product Editions

### Core
- Basic streaming ingest
- Simple transformations

### Pro
- Streaming workloads
- CDC capabilities
- Advanced transformations

### Advanced
- All Pro features
- Data quality expectations
- Advanced monitoring
- SLA guarantees

## Run-As User Configuration

**Purpose**: Change pipeline identity for continuity

### Recommended: Use Service Principals

Benefits:
- Independent of individual users
- Organizational continuity
- Consistent permissions

### Required Permissions

**Workspace:**
- CAN USE permission

**Source Code:**
- READ permission on notebooks/files

**Catalog:**
- USE CATALOG
- USE SCHEMA
- CREATE TABLE
- MODIFY (for existing tables)

## Source Code Configuration

### Supported Languages

```python
# Python
import dlt

@dlt.table()
def my_table():
    return spark.read.table("source")
```

```sql
-- SQL
CREATE OR REFRESH STREAMING TABLE my_table
AS SELECT * FROM source
```

### External Dependencies

**Python Libraries:**
```json
{
  "libraries": [
    {"pypi": {"package": "pandas==1.5.3"}},
    {"pypi": {"package": "requests"}}
  ]
}
```

**JAR Files:**
```json
{
  "libraries": [
    {"jar": "s3://bucket/libs/custom-lib.jar"}
  ]
}
```

### Import Modules

```python
# From workspace
from my_package import helper_functions

# From Git repositories
from git_repo.utils import transformations
```

## Pipeline Modes

### Continuous Mode
- Pipeline runs continuously
- Processes data as it arrives
- Higher compute costs
- Lower latency

**Use cases:**
- Real-time dashboards
- Time-sensitive applications
- Streaming analytics

### Triggered Mode
- Pipeline runs on schedule or manual trigger
- Processes batch data
- Lower compute costs
- Higher latency

**Use cases:**
- Daily batch processing
- Cost optimization
- Non-time-sensitive workloads

## Configuration Parameters

### Define Parameters

**UI Configuration:**
```json
{
  "catalog_name": "production",
  "schema_name": "sales",
  "source_path": "s3://bucket/data/"
}
```

### Use Parameters in Code

**Python:**
```python
catalog = spark.conf.get("catalog_name")
schema = spark.conf.get("schema_name")

@dlt.table()
def sales():
    return spark.read.table(f"{catalog}.{schema}.raw_sales")
```

**SQL:**
```sql
SET catalog_name = production;
SET schema_name = sales;

CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${schema_name}.sales
AS SELECT * FROM source
```

## Notifications

### Email Notifications

Configure alerts for:
- ✉️ Pipeline failures
- ✉️ Pipeline success
- ✉️ Data quality violations
- ✉️ Long-running updates

### Webhook Notifications

Integrate with:
- Slack
- PagerDuty
- Custom systems

## Runtime Channel

### Current Channel (Recommended)
- Latest stable features
- Regular updates
- Best performance

### Preview Channel
- Experimental features
- Early access
- May have bugs

### Legacy Channels
- Specific runtime versions
- For compatibility
- Not recommended for new pipelines

## Advanced Configuration

### Development Mode

**Enabled:**
- ⚡ Faster iterations
- 🔄 Reuses cluster
- 📊 Reduced costs
- ⚠️ Not for production

**Disabled (Production):**
- ✅ Full refresh capability
- ✅ Optimized performance
- ✅ Complete lineage tracking

### Target Schema

Specify where pipeline tables are created:

```json
{
  "target": "main.my_schema"
}
```

### Storage Location

Custom storage path for Delta tables:

```json
{
  "storage": "s3://bucket/pipelines/my-pipeline/"
}
```

## Complete Configuration Example

```json
{
  "name": "production_sales_pipeline",
  "target": "production.sales",
  "storage": "s3://data-lake/pipelines/sales/",
  "configuration": {
    "catalog_name": "production",
    "schema_name": "sales",
    "source_path": "s3://raw-data/sales/",
    "lookback_days": "7"
  },
  "libraries": [
    {"notebook": {"path": "/Pipelines/sales_transformations"}},
    {"pypi": {"package": "great-expectations"}}
  ],
  "clusters": [
    {
      "label": "default",
      "num_workers": 2,
      "autoscale": {
        "min_workers": 1,
        "max_workers": 5,
        "mode": "ENHANCED"
      },
      "node_type_id": "i3.xlarge",
      "driver_node_type_id": "i3.xlarge",
      "spark_conf": {
        "spark.sql.shuffle.partitions": "auto"
      },
      "aws_attributes": {
        "instance_profile_arn": "arn:aws:iam::123456789:instance-profile/DatabricksRole"
      }
    }
  ],
  "continuous": false,
  "development": false,
  "photon": true,
  "channel": "CURRENT",
  "edition": "ADVANCED",
  "notifications": [
    {
      "email_recipients": ["data-team@company.com"],
      "alerts": ["on-update-failure", "on-update-success"]
    }
  ]
}
```

## Best Practices

### ✅ DO

1. **Use serverless** for new pipelines
2. **Enable Photon** for better performance
3. **Configure service principals** for run-as identity
4. **Set up notifications** for failures
5. **Use parameters** for environment-specific configs
6. **Enable development mode** during development
7. **Tag clusters** for cost tracking

### ❌ DON'T

1. **Don't use development mode** in production
2. **Don't hardcode** environment-specific values
3. **Don't skip** notification setup
4. **Don't over-provision** compute resources
5. **Don't ignore** runtime channel updates

## Troubleshooting

### Pipeline Won't Start
- Check run-as user permissions
- Verify source code access
- Confirm compute configuration

### Slow Performance
- Enable Photon acceleration
- Increase worker count
- Use appropriate instance types
- Check for data skew

### High Costs
- Use triggered mode vs continuous
- Enable autoscaling
- Optimize worker configuration
- Consider serverless

### Permission Errors
- Verify Unity Catalog permissions
- Check run-as user grants
- Confirm instance profile IAM roles
