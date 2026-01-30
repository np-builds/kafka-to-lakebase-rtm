# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka to Lakebase — Real-Time Streaming
# MAGIC 
# MAGIC This notebook streams data from Kafka into Lakebase using Databricks Structured Streaming in Real-Time Mode.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Databricks Runtime 16.4+
# MAGIC - Dedicated cluster (not Shared)
# MAGIC - Real-Time Mode enabled via Spark config (see below)
# MAGIC - Kafka cluster with an active topic
# MAGIC - Lakebase instance created and running

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Configuration for Real-Time Mode
# MAGIC 
# MAGIC Add the following to your cluster's Spark config (Compute → Edit → Advanced Options → Spark):
# MAGIC 
# MAGIC ```
# MAGIC spark.databricks.streaming.realTimeMode.enabled true
# MAGIC ```
# MAGIC 
# MAGIC Restart the cluster after applying this setting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install psycopg --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.types import StructType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC 
# MAGIC Replace placeholder values with your own environment settings.

# COMMAND ----------

# =============================================================================
# STREAM SETTINGS
# =============================================================================

query_name = "<your_query_name>"              # e.g., "kafka_to_lakebase_stream"
trigger_interval = "5 seconds"                # Adjust based on latency requirements

# =============================================================================
# KAFKA CONNECTION
# Replace with your Kafka/Confluent Cloud credentials
# =============================================================================

kafka_bootstrap_servers = "<your_bootstrap_server>"    # e.g., "pkc-xxxxx.region.cloud:9092"
kafka_username = "<your_kafka_api_key>"
kafka_password = "<your_kafka_api_secret>"

# Topic to consume from
kafka_read_topic = "<your_topic_name>"                 # e.g., "machine_data"

# Checkpoint location for exactly-once processing
checkpoint_location = "/Volumes/<catalog>/<schema>/<volume>/streaming_checkpoint"

# COMMAND ----------

# =============================================================================
# LAKEBASE (POSTGRES) CONNECTION
# Replace with your Lakebase instance settings
# =============================================================================

lakebase_host = "<your_instance_id>.database.cloud.databricks.com"
lakebase_port = "5432"
lakebase_name = "<your_lakebase_instance_name>"        # e.g., "rtm-lakebase-writes"

# Database & table
db_name = "databricks_postgres"                        # Default Lakebase database
table_name = "<your_table_name>"                       # e.g., "machine_metrics"

# Authentication (use secrets management in production)
db_user = "<your_username>"
db_password = "<your_password>"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Kafka Configuration

# COMMAND ----------

# JAAS config for SASL authentication
jaas_config = (
    f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{kafka_username}" password="{kafka_password}";'
)

# Kafka reader options
kafka_read_config = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": jaas_config,
    "kafka.ssl.endpoint.identification.algorithm": "",
    "subscribe": kafka_read_topic,
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
    # "maxOffsetsPerTrigger": 1000  # Uncomment to limit records per batch
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from Kafka

# COMMAND ----------

kafka_stream_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_read_config)
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import ForEachWriter
# MAGIC 
# MAGIC This notebook contains the custom `LakebaseForeachWriter` class.  
# MAGIC Adjust the path to match your workspace structure.

# COMMAND ----------

# MAGIC %run ./02_lakebase_foreach_writer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize the Lakebase Writer

# COMMAND ----------

lakebase_writer = LakebaseForeachWriter(
    username=db_user,
    password=db_password,
    table=f"public.{table_name}",
    df=kafka_stream_df,
    lakebase_name=lakebase_name,
    host=lakebase_host,
    mode="insert",                  # Options: "insert", "upsert", "bulk-insert"
    # primary_keys=["id"],          # Required for "upsert" mode
    batch_size=100,
    batch_interval_ms=50,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start the Real-Time Stream

# COMMAND ----------

query = (
    kafka_stream_df.writeStream
    .foreach(lakebase_writer)
    .queryName(query_name)
    .trigger(realTime=trigger_interval)
    .outputMode("update")
    .option("checkpointLocation", checkpoint_location)
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor the Stream
# MAGIC 
# MAGIC Use the cell below to check stream status or stop it.

# COMMAND ----------

# Check stream status
# query.status

# Stop the stream (uncomment to use)
# query.stop()
