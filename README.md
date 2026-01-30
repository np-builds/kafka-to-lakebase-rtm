# Real-Time Streaming: Kafka to Lakebase

Stream data from Apache Kafka into Databricks Lakebase using Structured Streaming in Real-Time Mode.

## Overview

This repository contains two Databricks notebooks that work together to enable low-latency streaming from Kafka to Lakebase:

| Notebook | Description |
|----------|-------------|
| `01_kafka_to_lakebase.py` | Main streaming job — reads from Kafka, parses messages, and writes to Lakebase |
| `02_lakebase_foreach_writer.py` | Custom `ForEachWriter` class for efficient batched writes to Postgres/Lakebase |

## Prerequisites

- **Databricks Runtime:** 16.4 or higher
- **Cluster Mode:** Dedicated (not Shared)
- **Kafka:** A running Kafka cluster (e.g., Confluent Cloud) with an active topic
- **Lakebase:** A Databricks Lakebase instance

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/rtm-kafka-lakebase.git
```

### 2. Import into Databricks

**Option A: Import from GitHub**
1. In your Databricks workspace, click **Workspace** in the sidebar
2. Navigate to your target folder
3. Click **⋮** → **Import**
4. Select **URL** and paste the GitHub repository URL
5. Click **Import**

**Option B: Import files manually**
1. Download the `.py` files from this repository
2. In Databricks, navigate to your target folder
3. Click **⋮** → **Import**
4. Upload each file

### 3. Configure Your Cluster

Add the following to your cluster's Spark configuration:

```
spark.databricks.streaming.realTimeMode.enabled true
```

**To configure:**
1. Go to **Compute** → Select your cluster → **Edit**
2. Expand **Advanced Options** → **Spark**
3. Add the config above to the **Spark Config** text area
4. Click **Confirm** and restart the cluster

### 4. Update Configuration Values

Open `notebooks/01_kafka_to_lakebase.py` and replace all placeholder values:

```python
# Kafka
kafka_bootstrap_servers = "<your_bootstrap_server>"
kafka_username = "<your_kafka_api_key>"
kafka_password = "<your_kafka_api_secret>"
kafka_read_topic = "<your_topic_name>"

# Lakebase
lakebase_host = "<your_instance_id>.database.cloud.databricks.com"
lakebase_name = "<your_lakebase_instance_name>"
table_name = "<your_table_name>"
db_user = "<your_username>"
db_password = "<your_password>"

# Checkpoint
checkpoint_location = "/Volumes/<catalog>/<schema>/<volume>/streaming_checkpoint"
```

### 5. Customize the Schema

Modify the `message_schema` to match your Kafka message structure:

```python
message_schema = StructType([
    StructField("temperature_celsius", DoubleType()),
    StructField("vibration_level", DoubleType()),
    # Add your fields here...
])
```

### 6. Run the Notebooks

1. Attach both notebooks to your configured cluster
2. Run `01_kafka_to_lakebase` — it will automatically import the ForEachWriter

## File Structure

```
rtm-kafka-lakebase/
├── README.md
└── notebooks/
    ├── 01_kafka_to_lakebase.py        # Main streaming notebook
    └── 02_lakebase_foreach_writer.py  # ForEachWriter class
```

## ForEachWriter Features

The `LakebaseForeachWriter` class provides:

- **Batched writes** — Rows are queued and flushed in configurable batches (default: 1,000 rows)
- **Background worker** — Separate thread handles database writes to prevent blocking
- **Multiple write modes:**
  - `insert` — Standard inserts (fastest for append-only)
  - `upsert` — Insert or update on primary key conflict
  - `bulk-insert` — Uses Postgres `COPY` for maximum throughput
- **Flush triggers** — Batches flush on size threshold *or* time interval (default: 100ms)

## Configuration Options

### Writer Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `mode` | `"insert"` | Write mode: `insert`, `upsert`, or `bulk-insert` |
| `primary_keys` | `None` | Required for `upsert` mode |
| `batch_size` | `1000` | Rows per batch |
| `batch_interval_ms` | `100` | Max time between flushes |

### Example: Upsert Mode

```python
lakebase_writer = LakebaseForeachWriter(
    username=db_user,
    password=db_password,
    table=f"public.{table_name}",
    df=cleaned_df,
    host=lakebase_host,
    mode="upsert",
    primary_keys=["machine_id", "timestamp"],
    batch_size=500,
)
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `realTimeMode` not recognized | Ensure DBR 16.4+ and Spark config is set |
| Connection refused to Lakebase | Verify host, credentials, and network access |
| Schema mismatch errors | Update `message_schema` to match Kafka messages |
| Slow writes | Increase `batch_size` or try `bulk-insert` mode |

Original Lakebase forEach Writer here - https://github.com/christophergrant/lakebase-foreachwriter/blob/main/src/lakebase_foreachwriter/LakebaseForeachWriter.py
