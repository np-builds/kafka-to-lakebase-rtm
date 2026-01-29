# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase ForEachWriter
# MAGIC 
# MAGIC A custom `ForEachWriter` implementation for streaming writes to Lakebase (Postgres).
# MAGIC 
# MAGIC **Features:**
# MAGIC - Batched writes for throughput optimization
# MAGIC - Background worker thread for non-blocking I/O
# MAGIC - Support for `insert`, `upsert`, and `bulk-insert` modes
# MAGIC - Automatic type conversion from Spark to Python/Postgres types

# COMMAND ----------

# MAGIC %pip install psycopg --quiet

# COMMAND ----------

import logging
import queue
import threading
import time
from collections.abc import Sequence

import psycopg
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import ArrayType, MapType, StructType

# COMMAND ----------

def _build_conn_params(
    user: str, 
    password: str, 
    lakebase_name: str | None = None, 
    host: str | None = None
) -> dict:
    """Build connection parameters for Lakebase database."""
    
    if not host:
        if not lakebase_name:
            raise ValueError("Either host or lakebase_name must be provided")
        # Retrieve host from Databricks SDK if not provided
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        ws = WorkspaceClient(config=Config())
        host = ws.database.get_database_instance(lakebase_name).read_write_dns

    if not host:
        raise ValueError("host is required but was not provided and could not be retrieved.")

    return {
        "host": host,
        "port": 5432,
        "dbname": "databricks_postgres",
        "user": user,
        "password": password,
        "sslmode": "require",
    }

# COMMAND ----------

class LakebaseForeachWriter:
    """
    Writes streaming Spark DataFrames to Lakebase (Postgres) efficiently.
    
    Parameters
    ----------
    username : str
        Database username
    password : str
        Database password
    table : str
        Target table name (e.g., "public.my_table")
    df : DataFrame
        Streaming DataFrame (used for schema inference)
    lakebase_name : str, optional
        Lakebase instance name (alternative to host)
    host : str, optional
        Lakebase host address
    mode : str
        Write mode: "insert", "upsert", or "bulk-insert"
    primary_keys : list, optional
        Primary key columns (required for upsert mode)
    batch_size : int
        Number of rows per batch (default: 1000)
    batch_interval_ms : int
        Max time between flushes in milliseconds (default: 100)
    """

    def __init__(
        self,
        username: str,
        password: str,
        table: str,
        df: DataFrame,
        lakebase_name: str | None = None,
        host: str | None = None,
        mode: str = "insert",
        primary_keys: Sequence[str] | None = None,
        batch_size: int = 1000,
        batch_interval_ms: int = 100,
    ):
        # Validate schema for unsupported types
        unsupported = self._find_unsupported_fields(df.schema)
        if unsupported:
            raise ValueError(
                f"Unsupported field types found: {', '.join(unsupported)}. "
                "Please convert complex types to supported formats first."
            )

        self.username = username
        self.password = password
        self.table = table
        self.mode = mode.lower()
        self.columns = df.schema.names
        self.primary_keys = primary_keys if primary_keys else []
        self.batch_size = batch_size
        self.batch_interval_ms = batch_interval_ms

        self.conn_params = _build_conn_params(
            user=username,
            password=password,
            lakebase_name=lakebase_name,
            host=host,
        )

        # Runtime state (initialized in open())
        self.conn = None
        self.queue = None
        self.stop_event = None
        self.worker_thread = None
        self.batch = []
        self.last_flush = time.time()
        self.worker_error = None
        self.sql = self._build_sql()

    # -------------------------------------------------------------------------
    # ForEachWriter Interface
    # -------------------------------------------------------------------------

    def open(self, partition_id: int, epoch_id: int) -> bool:
        """Called when a new partition starts processing."""
        try:
            self.partition_id = partition_id
            self.epoch_id = epoch_id
            self.conn = psycopg.connect(**self.conn_params)
            self.queue = queue.SimpleQueue()
            self.stop_event = threading.Event()
            self.batch = []
            self.last_flush = time.time()
            self.worker_error = None
            
            # Start background worker for async writes
            self.worker_thread = threading.Thread(target=self._worker, daemon=True)
            self.worker_thread.start()
            
            logging.info(f"[{partition_id}|{epoch_id}] Opening writer for table {self.table}")
            return True
        except Exception as e:
            logging.error(f"Failed to open writer: {e}")
            return False

    def process(self, row: Row | tuple):
        """Called for each row in the stream."""
        if self.worker_error:
            raise Exception(f"Worker failed: {self.worker_error}")

        if isinstance(row, Row):
            row_data = tuple(self._to_python(row[col]) for col in self.columns)
        else:
            row_data = tuple(self._to_python(v) for v in row)
        
        self.queue.put(row_data)

    def close(self, error: Exception | None):
        """Called when partition processing completes."""
        try:
            if self.stop_event:
                self.stop_event.set()
            if self.worker_thread and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=5.0)
            if self.queue and self.queue.qsize() > self.batch_size * 5:
                logging.warning(
                    f"[{self.partition_id}|{self.epoch_id}] Large queue remaining: {self.queue.qsize()}"
                )
            self._flush_remaining()
        finally:
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass
        logging.info(f"[{self.partition_id}|{self.epoch_id}] Writer closed")

    # -------------------------------------------------------------------------
    # Internal Methods
    # -------------------------------------------------------------------------

    def _worker(self):
        """Background thread that batches and flushes rows."""
        while not self.stop_event.is_set():
            try:
                # Collect rows until batch is full
                while len(self.batch) < self.batch_size:
                    try:
                        item = self.queue.get(timeout=0.01)
                        self.batch.append(item)
                    except queue.Empty:
                        break

                # Flush if batch is full or interval elapsed
                if len(self.batch) >= self.batch_size or (self.batch and self._time_to_flush()):
                    self._flush_batch()

                time.sleep(0.0001)
            except Exception as e:
                logging.error(f"Worker error: {e}")
                self.worker_error = str(e)
                break

    def _flush_batch(self):
        """Write current batch to database."""
        if not self.batch:
            return
        try:
            perf_start = time.time()
            with self.conn.cursor() as cur:
                if self.mode == "bulk-insert":
                    cols = ", ".join(self.columns)
                    with cur.copy(f"COPY {self.table} ({cols}) FROM STDIN") as copy:
                        for row in self.batch:
                            copy.write_row(row)
                else:
                    cur.executemany(self.sql, self.batch)

            self.conn.commit()
            batch_size = len(self.batch)
            self.batch = []
            self.last_flush = time.time()
            perf_time = (time.time() - perf_start) * 1000
            logging.info(
                f"[{self.partition_id}|{self.epoch_id}] Flushed {batch_size} rows in {perf_time:.1f}ms"
            )
        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass
            raise

    def _flush_remaining(self):
        """Drain the queue and flush any remaining rows."""
        while True:
            try:
                self.batch.append(self.queue.get_nowait())
            except queue.Empty:
                break
        if self.batch:
            self._flush_batch()

    def _time_to_flush(self) -> bool:
        """Check if enough time has passed to trigger a flush."""
        return (time.time() - self.last_flush) * 1000 >= self.batch_interval_ms

    def _build_sql(self) -> str | None:
        """Build the SQL statement based on write mode."""
        cols = ", ".join(self.columns)
        placeholders = ", ".join(["%s"] * len(self.columns))

        if self.mode == "insert":
            return f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders})"
        
        elif self.mode == "upsert":
            if not self.primary_keys:
                raise ValueError("primary_keys required for upsert mode")
            pk_cols = ", ".join(self.primary_keys)
            update_cols = ", ".join(
                f"{c} = EXCLUDED.{c}" for c in self.columns if c not in self.primary_keys
            )
            return f"""
                INSERT INTO {self.table} ({cols}) VALUES ({placeholders})
                ON CONFLICT ({pk_cols}) DO UPDATE SET {update_cols}
            """
        
        elif self.mode == "bulk-insert":
            return None  # Uses COPY command instead
        
        else:
            raise ValueError(f"Invalid mode: {self.mode}. Use 'insert', 'upsert', or 'bulk-insert'.")

    def _to_python(self, value):
        """Convert Spark/NumPy types to native Python types."""
        if value is None:
            return None

        type_name = type(value).__name__

        # Boolean (check first — bool is subclass of int in Python)
        if type_name in ('bool', 'bool_', 'boolean') or isinstance(value, bool):
            return True if value else False

        # Integer types
        if type_name in ('int', 'int8', 'int16', 'int32', 'int64', 'int_', 'long'):
            return int(value)

        # Float types
        if type_name in ('float', 'float16', 'float32', 'float64', 'float_', 'double'):
            return float(value)

        # String
        if type_name in ('str', 'str_'):
            return str(value)

        return value

    @staticmethod
    def _find_unsupported_fields(schema: StructType) -> list[str]:
        """Check schema for unsupported complex types."""
        unsupported = []
        unsupported_types = (StructType, MapType)
        for field in schema.fields:
            if isinstance(field.dataType, unsupported_types):
                unsupported.append(field.name)
            elif isinstance(field.dataType, ArrayType) and isinstance(
                field.dataType.elementType, unsupported_types
            ):
                unsupported.append(field.name)
        return unsupported

# COMMAND ----------

print("LakebaseForeachWriter class loaded successfully!")
