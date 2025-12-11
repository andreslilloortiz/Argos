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

# --- SPARK SUBMIT ---
# docker exec -it argos_spark_master /opt/spark/bin/spark-submit --jars /opt/spark/jars/clickhouse-jdbc.jar /opt/spark_scripts/etl_job.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit
from pyspark.sql.types import DoubleType, BooleanType

# 1. INITIALIZE SPARK
# Configure JDBC driver for ClickHouse connection
spark = SparkSession.builder \
    .appName("ArgosCryptoETL") \
    .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc.jar") \
    .getOrCreate()

# 2. EXTRACT (Read Data)
# Read all files in the directory (using wildcard * to ignore extensions)
# We let Spark infer the schema from the JSON content automatically
raw_df = spark.read.json("/opt/shared_data/*")

# Check if dataframe is empty to avoid unnecessary processing errors
if raw_df.isEmpty():
    print(">>> No new data to process.")
    spark.stop()
    exit()

# 3. TRANSFORM
# Map REST API fields (time, qty, etc.) to our ClickHouse table schema
transformed_df = raw_df.select(
    # API returns milliseconds, convert to seconds for timestamp
    (col("time") / 1000).cast("timestamp").alias("event_time"),

    # REST API doesn't include symbol in the body, so we hardcode it for now
    lit("BTCUSDT").alias("symbol"),

    col("price").cast(DoubleType()).alias("price"),
    col("qty").cast(DoubleType()).alias("quantity"),

    # REST API provides calculated volume as 'quoteQty'
    col("quoteQty").cast(DoubleType()).alias("quote_asset_volume"),

    # Cast boolean to integer (0 or 1) for ClickHouse UInt8 storage
    col("isBuyerMaker").cast("int").alias("is_buyer_maker")
)

# 4. LOAD (Write to ClickHouse)
# Write the transformed dataframe to 'trades' table via JDBC
print(">>> Writing to ClickHouse...")
transformed_df.write \
    .format("jdbc") \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("url", "jdbc:clickhouse://clickhouse:8123/argos") \
    .option("dbtable", "trades") \
    .option("user", "default") \
    .option("password", "Argos123!") \
    .mode("append") \
    .save()

print(">>> SUCCESS: Data loaded into ClickHouse!")
spark.stop()