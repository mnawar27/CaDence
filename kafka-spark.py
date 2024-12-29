###  Old code  ###

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, IntegerType
# import os

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


# # Initialize SparkSession
# spark = SparkSession.builder \
#     .appName("Kafka-PySpark-Consumer") \
#     .master("local[*]") \
#     .getOrCreate()

# # Kafka topic and server
# kafka_topic = "testtopic"
# kafka_bootstrap_servers = "localhost:9092"

# # Define schema for JSON messages
# schema = StructType([
#     StructField("number", IntegerType(), True)
# ])

# # Read messages from Kafka
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#     .option("subscribe", kafka_topic) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Extract and deserialize the JSON value
# messages = df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")

# # Apply transformations
# transformed = messages.withColumn("number_squared", col("number") * col("number"))

# # Print to terminal
# query = transformed.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()

###  New code  ###
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

# Set the Spark package for Kafka integration
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Kafka Session Aggregation") \
    .master("local[*]") \
    .getOrCreate()

# Define schema for the JSON messages
schema = StructType([
    StructField("sessionId", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("userAgent", StringType(), True)
])

# Kafka configuration
kafka_topic = "testtopic"
kafka_bootstrap_servers = "localhost:9092"

# Read messages from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON data and select relevant fields
messages = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Define platform detection logic
transformed = messages \
    .withColumn("platform", when(col("userAgent").contains("Windows"), "Windows")
                .when(col("userAgent").contains("Macintosh"), "Mac")
                .when(col("userAgent").contains("Linux"), "Linux")
                .otherwise("Other"))

# Aggregate data by sessionId
aggregated_df = transformed.groupBy("sessionId").agg(
    count(when(col("gender") == "M", 1)).alias("male_count"),
    count(when(col("gender") == "F", 1)).alias("female_count"),
    count(when(col("auth") == "Logged In", 1)).alias("active_users"),
    count(when(col("auth") == "Logged Out", 1)).alias("inactive_users"),
    count(when(col("platform") == "Windows", 1)).alias("windows_users"),
    count(when(col("platform") == "Mac", 1)).alias("mac_users"),
    count(when(col("platform") == "Linux", 1)).alias("linux_users")
)

# Output results to the console
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()