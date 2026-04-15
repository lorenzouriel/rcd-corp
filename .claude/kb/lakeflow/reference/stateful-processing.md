# Stateful Processing in Lakeflow

> Source: https://docs.databricks.com/aws/en/dlt/stateful-processing

## Overview

Stateful processing enables complex streaming operations that maintain state across micro-batches, including windowed aggregations, stream-stream joins, and deduplication.

## Watermarks

### What is a Watermark?

**A watermark is a time-based threshold for processing data when performing stateful operations.**

Watermarks help:
- Manage unordered streaming data
- Control late data processing
- Determine when to emit results
- Clean up old state

### Defining Watermarks

**Python:**
```python
import dlt
from pyspark.sql import functions as F

@dlt.table()
def with_watermark():
    return (
        dlt.read_stream("events")
        .withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "1 hour"))
        .agg(F.count("*").alias("event_count"))
    )
```

**SQL:**
```sql
CREATE OR REFRESH STREAMING TABLE windowed_events AS
SELECT
    window(event_time, '1 hour') as time_window,
    COUNT(*) as event_count
FROM STREAM(events)
WATERMARK event_time DELAY OF INTERVAL 10 MINUTES
GROUP BY window(event_time, '1 hour')
```

### Choosing Watermark Threshold

**Tradeoffs:**
- **Shorter watermark** (e.g., 1 minute)
  - ✅ Lower latency
  - ❌ May drop more late data

- **Longer watermark** (e.g., 1 hour)
  - ✅ Captures more late data
  - ❌ Higher latency
  - ❌ More state to maintain

**Best practice:** Start with longer watermark, monitor late data, then optimize.

## Stream-Stream Joins

### Requirements

Stream-stream joins **require watermarks on both sources** to determine when no further matches can be made.

### Time Interval Constraint

Must define a time interval to limit how far back to look for matches.

### Python Example

```python
import dlt
from pyspark.sql import functions as F

@dlt.table()
def clicks():
    return (
        dlt.read_stream("click_events")
        .withWatermark("click_time", "10 minutes")
    )

@dlt.table()
def impressions():
    return (
        dlt.read_stream("impression_events")
        .withWatermark("impression_time", "10 minutes")
    )

@dlt.table()
def matched_events():
    clicks_df = dlt.read_stream("clicks")
    impressions_df = dlt.read_stream("impressions")

    return (
        clicks_df.alias("c")
        .join(
            impressions_df.alias("i"),
            F.expr("""
                c.user_id = i.user_id AND
                c.click_time >= i.impression_time AND
                c.click_time <= i.impression_time + INTERVAL 1 HOUR
            """)
        )
    )
```

### Global Watermark

**The global watermark is based on the slowest stream:**
- If stream A watermark = 10:00
- If stream B watermark = 09:50
- Global watermark = 09:50 (slowest)

## Windowed Aggregations

### Tumbling Windows (Fixed)

Non-overlapping, fixed-size time intervals.

```python
@dlt.table()
def hourly_metrics():
    return (
        dlt.read_stream("events")
        .withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "1 hour"))
        .agg(
            F.count("*").alias("event_count"),
            F.sum("revenue").alias("total_revenue")
        )
    )
```

**Visualization:**
```
Events:  |-------|-------|-------|
Windows: [0-1hr] [1-2hr] [2-3hr]
```

### Sliding Windows

Fixed-size windows that can overlap.

```python
@dlt.table()
def sliding_window_metrics():
    return (
        dlt.read_stream("events")
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window(
                "event_time",
                "1 hour",      # Window duration
                "30 minutes"   # Slide interval
            )
        )
        .agg(F.count("*").alias("event_count"))
    )
```

**Visualization:**
```
Events:  |--------------|
Windows: [0:00-1:00]
         └─[0:30-1:30]
            └─[1:00-2:00]
```

### SQL Syntax

```sql
-- Tumbling window
CREATE OR REFRESH STREAMING TABLE hourly_metrics AS
SELECT
    window(event_time, '1 hour') as time_window,
    COUNT(*) as event_count,
    SUM(revenue) as total_revenue
FROM STREAM(events)
WATERMARK event_time DELAY OF INTERVAL 10 MINUTES
GROUP BY window(event_time, '1 hour')

-- Sliding window
CREATE OR REFRESH STREAMING TABLE sliding_metrics AS
SELECT
    window(event_time, '1 hour', '30 minutes') as time_window,
    COUNT(*) as event_count
FROM STREAM(events)
WATERMARK event_time DELAY OF INTERVAL 10 MINUTES
GROUP BY window(event_time, '1 hour', '30 minutes')
```

## Deduplication

### dropDuplicatesWithinWatermark()

Remove duplicate records within a watermark threshold.

```python
@dlt.table()
def deduplicated_events():
    return (
        dlt.read_stream("events")
        .withWatermark("event_time", "1 hour")
        .dropDuplicatesWithinWatermark(["event_id"])
    )
```

### With Event Time Ordering

Process initial snapshots in chronological order.

```python
@dlt.table()
def ordered_deduplicated():
    return (
        dlt.read_stream("events")
        .withWatermark("event_time", "1 hour")
        .dropDuplicatesWithinWatermark(
            ["event_id"],
            withEventTimeOrder=True
        )
    )
```

**Use cases:**
- ✅ Initial snapshot processing
- ✅ Maintaining temporal order
- ⚠️ Higher latency (waits for watermark)

## State Store Optimization

### RocksDB State Store

For large intermediate state, use RocksDB instead of default in-memory state store.

**Configuration:**

```json
{
  "configuration": {
    "spark.sql.streaming.stateStore.providerClass": "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
  }
}
```

**Benefits:**
- 📦 Handles larger state (GBs to TBs)
- 💾 Disk-based storage
- 🚀 Better performance for large state
- 💰 Lower memory costs

**When to use:**
- State size > 100MB per partition
- Many distinct keys in aggregations
- Long watermark intervals
- Complex windowed operations

### State Store Monitoring

```python
# Check state store metrics in Spark UI:
# - Number of keys in state
# - State store size
# - State store commit time
# - State store memory usage
```

## Complete Example: Session Analytics

```python
import dlt
from pyspark.sql import functions as F

# Bronze: Raw events
@dlt.table()
def raw_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://bucket/events/")
    )

# Silver: Deduplicated with watermark
@dlt.table()
def deduplicated_events():
    return (
        dlt.read_stream("raw_events")
        .withWatermark("event_time", "30 minutes")
        .dropDuplicatesWithinWatermark(["event_id", "user_id"])
    )

# Gold: Session windows (gap-based)
@dlt.table()
def user_sessions():
    return (
        dlt.read_stream("deduplicated_events")
        .withWatermark("event_time", "30 minutes")
        .groupBy(
            "user_id",
            F.session_window("event_time", "15 minutes")  # 15min gap = new session
        )
        .agg(
            F.count("*").alias("events_in_session"),
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
            F.collect_list("page_url").alias("pages_visited")
        )
    )

# Gold: Hourly aggregations
@dlt.table()
def hourly_activity():
    return (
        dlt.read_stream("deduplicated_events")
        .withWatermark("event_time", "1 hour")
        .groupBy(
            F.window("event_time", "1 hour"),
            "user_type"
        )
        .agg(
            F.countDistinct("user_id").alias("active_users"),
            F.count("*").alias("total_events"),
            F.avg("session_duration").alias("avg_session_duration")
        )
    )
```

## Best Practices

### ✅ DO

1. **Choose appropriate watermarks**
   - Start conservative (longer)
   - Monitor late data metrics
   - Adjust based on SLA

2. **Use RocksDB for large state**
   - Enable when state > 100MB/partition
   - Monitor state store metrics
   - Test performance impact

3. **Optimize window sizes**
   - Align with business requirements
   - Balance granularity vs performance
   - Consider data volume

4. **Monitor watermark lag**
   - Track watermark progression
   - Alert on stuck watermarks
   - Investigate slow sources

5. **Test with late data**
   - Simulate late arrivals
   - Verify watermark behavior
   - Validate results correctness

### ❌ DON'T

1. **Don't use tiny watermarks** without understanding late data
2. **Don't forget watermarks** on stream-stream joins
3. **Don't use huge windows** without state management
4. **Don't ignore state growth** over time
5. **Don't mix watermarked and non-watermarked** streams incorrectly

## Performance Tuning

### State Partitioning

```python
# Increase partitions for large state
.repartition(200, "partition_key")
.withWatermark("event_time", "10 minutes")
.groupBy("partition_key", F.window("event_time", "1 hour"))
```

### State Cleanup

```python
# Configure state cleanup
{
  "configuration": {
    "spark.sql.streaming.minBatchesToRetain": "10",
    "spark.sql.streaming.stateStore.maintenanceInterval": "60s"
  }
}
```

### Checkpoint Management

```python
# Efficient checkpoint location
{
  "configuration": {
    "pipelines.checkpointLocation": "s3://bucket/checkpoints/"
  }
}
```

## Troubleshooting

### Watermark Not Progressing

**Symptoms:**
- Watermark stuck at old timestamp
- State growing indefinitely
- No output from windowed operations

**Causes:**
- One source stopped sending data
- Event time column has nulls
- Event time column has future timestamps

**Solutions:**
```python
# Filter out nulls
.filter(F.col("event_time").isNotNull())

# Cap future timestamps
.withColumn(
    "event_time",
    F.when(
        F.col("event_time") > F.current_timestamp(),
        F.current_timestamp()
    ).otherwise(F.col("event_time"))
)
```

### State Store Growth

**Symptoms:**
- Increasing memory usage
- Slow micro-batch processing
- OOM errors

**Solutions:**
1. Enable RocksDB state store
2. Reduce watermark interval
3. Increase state cleanup frequency
4. Partition data better

### Late Data Being Dropped

**Symptoms:**
- Missing events in aggregations
- Lower counts than expected

**Solutions:**
1. Increase watermark threshold
2. Monitor late data metrics
3. Consider separate late data pipeline

## Metrics and Monitoring

### Key Metrics

```python
# Monitor in Spark UI and pipeline metrics:

# Watermark metrics
- Current watermark value
- Watermark lag (current time - watermark)
- Late events count

# State store metrics
- Number of state store keys
- State store size (MB)
- State store commit duration
- State store memory usage

# Processing metrics
- Input rate
- Processing rate
- Batch duration
- State cleanup duration
```

### Alerting Thresholds

```python
# Set alerts for:
- Watermark lag > 1 hour
- State store size > 10GB
- Late events > 5% of total
- Batch duration > 10 minutes
```

## Advanced Patterns

### Multi-Level Aggregations

```python
# Pre-aggregate then window
@dlt.table()
def pre_aggregated():
    return (
        dlt.read_stream("events")
        .groupBy("user_id", F.window("event_time", "1 minute"))
        .agg(F.count("*").alias("minute_count"))
    )

@dlt.table()
def hourly_from_minutes():
    return (
        dlt.read_stream("pre_aggregated")
        .withWatermark("window.end", "5 minutes")
        .groupBy(F.window("window.end", "1 hour"))
        .agg(F.sum("minute_count").alias("hourly_count"))
    )
```

### Custom State Management

```python
# Use mapGroupsWithState for custom state logic
from pyspark.sql.streaming import GroupState

def update_session_state(key, values, state):
    # Custom stateful logic
    if state.hasTimedOut:
        # Emit final session
        return key, state.get()

    # Update state
    current = state.getOption or {"count": 0}
    current["count"] += sum(1 for _ in values)
    state.update(current)
    state.setTimeoutTimestamp(...)

    return key, current

@dlt.table()
def custom_sessions():
    return (
        dlt.read_stream("events")
        .groupByKey(lambda x: x.user_id)
        .mapGroupsWithState(update_session_state, ...)
    )
```

## References

- **Stateful Processing Docs**: [AWS](https://docs.databricks.com/aws/en/dlt/stateful-processing) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dlt/stateful-processing)
- **RocksDB State Store**: Spark Structured Streaming documentation
- **Watermarking Guide**: Apache Spark documentation
