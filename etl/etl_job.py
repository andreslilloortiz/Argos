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

# spark-submit --jars /opt/spark/jars/clickhouse-jdbc.jar /opt/spark_scripts/etl_job.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

# 1. INITIALIZE SPARK
# We configure the JDBC driver here to allow communication with ClickHouse.
spark = SparkSession.builder \
    .appName("ArgosETL") \
    .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc.jar") \
    .getOrCreate()

# 2. DEFINE SCHEMA
# Spark needs to know the structure of the JSON data coming from the Binance API.
# Mapping based on Binance WebSocket/API standard:
# s: Symbol, p: Price, q: Quantity, T: Trade Time, m: Is Buyer Maker
schema = StructType([
    StructField("s", StringType(), True),
    StructField("p", StringType(), True),
    StructField("q", StringType(), True),
    StructField("T", LongType(), True),
    StructField("m", BooleanType(), True)
])

# 3. EXTRACT (Read Data)
# Read all JSON files accumulated in the shared Data Lake directory.
# We treat this as a batch job for now (reading the current snapshot of files).
raw_df = spark.read.json("/opt/shared_data/*.json")

# 4. TRANSFORM
# Select relevant columns, cast data types, and rename to match the ClickHouse table schema.
transformed_df = raw_df.select(
    (col("T") / 1000).cast("timestamp").alias("event_time"), # Convert ms to timestamp
    col("s").alias("symbol"),
    col("p").cast(DoubleType()).alias("price"),
    col("q").cast(DoubleType()).alias("quantity"),
    (col("p") * col("q")).alias("quote_asset_volume"),       # Calculate total volume (Price * Quantity)
    col("m").cast("int").alias("is_buyer_maker")             # Convert boolean to 0/1 for DB storage
)

# 5. LOAD (Write to ClickHouse)
# Write the transformed dataframe to the 'trades' table via JDBC.
transformed_df.write \
    .format("jdbc") \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("url", "jdbc:clickhouse://clickhouse:8123/argos") \
    .option("dbtable", "trades") \
    .option("user", "default") \
    .option("password", "") \
    .mode("append") \
    .save()

print(">>> Data successfully loaded into ClickHouse!")
spark.stop()