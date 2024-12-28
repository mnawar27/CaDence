from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Kafka-PySpark-Consumer") \
    .master("local[*]") \
    .getOrCreate()

# Kafka topic and server
kafka_topic = "testtopic"
kafka_bootstrap_servers = "localhost:9092"

# Define schema for JSON messages
schema = StructType([
    StructField("number", IntegerType(), True)
])

# Read messages from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Extract and deserialize the JSON value
messages = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Apply transformations
transformed = messages.withColumn("number_squared", col("number") * col("number"))

# Print to terminal
query = transformed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
