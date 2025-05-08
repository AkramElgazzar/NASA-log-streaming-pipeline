import os
import time

# Set exact package versions for Spark-Kafka integration
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.apache.kafka:kafka-clients:2.4.1 pyspark-shell'

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, window, count, current_timestamp
from pyspark.sql.types import IntegerType

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("ApacheLogAnalysis60SecIntervals") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

# Set log level to reduce output
spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "apache_logs") \
    .option("startingOffsets", "latest") \
    .load()

# Extract log line from Kafka value
logs_df = kafka_df.selectExpr(
    "CAST(value AS STRING) as log_line",
    "CAST(timestamp AS TIMESTAMP) as kafka_timestamp"
)

# Add current timestamp for windowing
logs_with_time = logs_df.withColumn("processing_time", current_timestamp())

# Parse Apache log lines using regex
parsed_logs = logs_with_time \
    .withColumn("ip", regexp_extract(col("log_line"), r"^(\S+)", 1)) \
    .withColumn("timestamp", regexp_extract(col("log_line"), r"\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} [+\-]\d{4})\]", 1)) \
    .withColumn("method", regexp_extract(col("log_line"), r"\"(\S+)", 1)) \
    .withColumn("endpoint", regexp_extract(col("log_line"), r"\"(?:\S+) (\S+)", 1)) \
    .withColumn("protocol", regexp_extract(col("log_line"), r"\"(?:\S+) (?:\S+) (\S+)\"", 1)) \
    .withColumn("status", regexp_extract(col("log_line"), r" (\d{3}) ", 1).cast(IntegerType())) \
    .withColumn("bytes", regexp_extract(col("log_line"), r" (\d+|-)$", 1).cast(IntegerType()))

# Display schema for verification
parsed_logs.printSchema()

# Windowed aggregations
windowed_logs = parsed_logs \
    .groupBy(window(col("processing_time"), "60 seconds")) \
    .count() \
    .withColumnRenamed("count", "total_requests") \
    .orderBy(col("window").desc())

status_distribution = parsed_logs \
    .groupBy(window(col("processing_time"), "60 seconds"), "status") \
    .count() \
    .orderBy(col("window").desc(), "status")

endpoint_traffic = parsed_logs \
    .groupBy(window(col("processing_time"), "60 seconds"), "endpoint") \
    .count() \
    .orderBy(col("window").desc(), col("count").desc())

method_distribution = parsed_logs \
    .groupBy(window(col("processing_time"), "60 seconds"), "method") \
    .count() \
    .orderBy(col("window").desc(), col("count").desc())

ip_traffic = parsed_logs \
    .groupBy(window(col("processing_time"), "60 seconds"), "ip") \
    .count() \
    .orderBy(col("window").desc(), col("count").desc())

error_rates = parsed_logs \
    .filter(col("status") >= 400) \
    .groupBy(window(col("processing_time"), "60 seconds"), "status") \
    .count() \
    .orderBy(col("window").desc(), "status")

# Function to start streaming query
def start_query(df, name, num_rows=20):
    return df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="60 seconds") \
        .queryName(name) \
        .option("truncate", "false") \
        .option("numRows", num_rows) \
        .start()

# Stop existing queries with same names
for query in spark.streams.active:
    if query.name in ["total_requests", "status_distribution", "endpoint_traffic", "method_distribution", "ip_traffic", "error_rates"]:
        print(f"Stopping existing query: {query.name}")
        query.stop()

# Start streaming queries
queries = [
    start_query(windowed_logs, "total_requests"),
    start_query(status_distribution, "status_distribution"),
    start_query(endpoint_traffic, "endpoint_traffic"),
    start_query(method_distribution, "method_distribution"),
    start_query(ip_traffic, "ip_traffic"),
    start_query(error_rates, "error_rates")
]

# Print active queries
for query in queries:
    print(f"Query '{query.name}' is active.")
