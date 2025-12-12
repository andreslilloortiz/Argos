# This file is part of Argos.

# Copyright (C) 2025 Andrés Lillo Ortiz

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

# This file is part of Argos.
# Copyright (C) 2025 Andrés Lillo Ortiz

from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, lit # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType # type: ignore

# 1. INITIALIZE SPARK
# Configure the session with the ClickHouse JDBC driver
# We set the log level to WARN to keep the console clean from INFO noise
spark = SparkSession.builder \
    .appName("ArgosRealTime") \
    .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. DEFINE SCHEMA (Mandatory for Streaming)
# In Structured Streaming, Spark MUST know the schema beforehand
# (it cannot infer it by reading existing files).
# This schema matches the JSON structure sent by NiFi from Binance.
json_schema = StructType([
    StructField("id", LongType(), True),
    StructField("price", StringType(), True),
    StructField("qty", StringType(), True),
    StructField("quoteQty", StringType(), True),
    StructField("time", LongType(), True),
    StructField("isBuyerMaker", BooleanType(), True),
    StructField("isBestMatch", BooleanType(), True)
])

# 3. READ STREAM (Extract)
# We use .readStream instead of .read to enable continuous monitoring.
raw_stream = spark.readStream \
    .schema(json_schema) \
    .json("/opt/shared_data")  # Monitored directory

# 4. DEFINE PROCESSING LOGIC (Transform & Load)
# This function is executed automatically for every new "micro-batch" of files detected.
def process_micro_batch(df, epoch_id):
    if df.isEmpty():
        return

    print(f">>> Processing micro-batch ID: {epoch_id}")

    # --- TRANSFORM ---
    # Select relevant columns, cast types, and rename to match ClickHouse table schema
    transformed_df = df.select(
        # Convert milliseconds to timestamp
        (col("time") / 1000).cast("timestamp").alias("event_time"),

        # Hardcode symbol (REST API limitation, assuming BTCUSDT)
        lit("BTCUSDT").alias("symbol"),

        col("price").cast(DoubleType()).alias("price"),
        col("qty").cast(DoubleType()).alias("quantity"),
        col("quoteQty").cast(DoubleType()).alias("quote_asset_volume"),

        # Convert boolean to UInt8 (0/1) for ClickHouse compatibility
        col("isBuyerMaker").cast("int").alias("is_buyer_maker")
    )

    # --- LOAD (ClickHouse) ---
    # JDBC URL includes credentials and 'dialect=ansi' to support Spark's quoting style
    jdbc_url = "jdbc:clickhouse://clickhouse:8123/argos?user=default&password=Password123!"

    try:
        transformed_df.write \
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", jdbc_url) \
            .option("dbtable", "trades") \
            .option("isolationLevel", "NONE") \
            .option("batchsize", "5000") \
            .mode("append") \
            .save()
        print(f">>> SUCCESS: Batch {epoch_id} loaded into ClickHouse.")
    except Exception as e:
        print(f"!!! Error writing batch {epoch_id}: {e}")

# 5. START STREAMING
# Start the stream. 'checkpointLocation' is critical for fault tolerance.
# It tracks which files have been processed to ensure exactly-once semantics upon restart.
query = raw_stream.writeStream \
    .foreachBatch(process_micro_batch) \
    .option("checkpointLocation", "/opt/spark_checkpoints") \
    .trigger(processingTime='10 seconds') \
    .start()

print(">>> Argos Streaming initialized. Listening for data...")
# Keep the process alive indefinitely
query.awaitTermination()