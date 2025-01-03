from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .getOrCreate()

# Set log level to WARN to reduce log noise
spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "testing12") \
    .load()

# Convert the binary 'value' column to a string
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Parse the 'value' column as JSON
df_parsed = df.select(
    col("key").alias("sessionId"),  # Use Kafka key as sessionId
    col("value")
)

# Now, parse the JSON data inside 'value'
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for your JSON data
schema = StructType([
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("ts", IntegerType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", IntegerType(), True),
    StructField("lat", IntegerType(), True),
    StructField("userId", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", IntegerType(), True),
])

# Parse the value column using the schema
df_parsed = df_parsed.withColumn("json_data", from_json("value", schema))

# Now you can access the individual fields in the JSON
df_parsed = df_parsed.select(
    "sessionId",
    "json_data.gender"
)

# Perform the aggregation to count males and females
result = df_parsed \
    .groupBy("sessionId") \
    .agg(
        count(when(col("gender") == "M", 1)).alias("male_count"),
        count(when(col("gender") == "F", 1)).alias("female_count")
    )

# Write the results to the console
query = result \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
