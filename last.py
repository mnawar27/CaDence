import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, udf,count
from pyspark.sql.types import StringType

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName('Table Processing') \
    .master('local[*]') \
    .config("spark.driver.extraClassPath", "/Users/mnawar/Desktop/passion/CaDence/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar") \
    .getOrCreate()

# Define lists for each time zone, containing state abbreviations
central_time_zone = ["AL", "AR", "IL", "IA", "KS", "KY", "LA", "MN", "MS", "MO", "NE", "ND", "OK", "SD", "TN", "TX", "WI"]
eastern_time_zone = ["CT", "DE", "FL", "GA", "HI", "ME", "MD", "MA", "MI", "NC", "NH", "NJ", "NY", "OH", "PA", "RI", "SC", "VT", "VA", "WA", "WV"]
mountain_time_zone = ["AZ", "CO", "ID", "MT", "NM", "UT", "WY"]
pacific_time_zone = ["AK", "CA", "NV", "OR", "WA"]
hawaii_aleutian_time_zone = ["HI"]

# UDF to assign time zone based on state abbreviation
def get_time_zone(state_abbr):
    if state_abbr in central_time_zone:
        return "Central Time Zone (CT)"
    elif state_abbr in eastern_time_zone:
        return "Eastern Time Zone (ET)"
    elif state_abbr in mountain_time_zone:
        return "Mountain Time Zone (MT)"
    elif state_abbr in pacific_time_zone:
        return "Pacific Time Zone (PT)"
    elif state_abbr in hawaii_aleutian_time_zone:
        return "Hawaii-Aleutian Time Zone (HAT)"
    else:
        return f"Time zone for state abbreviation '{state_abbr}' not found."

# Register UDF in Spark
get_time_zone_udf = udf(get_time_zone, StringType())

def process_table(table_name):
    """Process a single table to calculate gender, level counts, top song, and user counts in time zones."""
    try:
        # Read data from the table
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:mysql://localhost/cadenceDB") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", "mnawar") \
            .option("password", "thisistheway") \
            .load()

        # Add a time zone column based on the state abbreviation
        df = df.withColumn("time_zone", get_time_zone_udf(col("state")))

        # Get the distinct count of users per time zone
        user_counts_by_time_zone = df.groupBy("time_zone").agg(countDistinct("userId").alias("distinct_user_count"))

        # Get distinct userId, gender, and level
        unique_users_gender_level = df.select("userId", "gender", "level").distinct()

        # Calculate gender breakdown
        gender_counts = unique_users_gender_level.groupBy("gender").count()

        # Calculate level breakdown
        level_counts = unique_users_gender_level.groupBy("level").count()

        # Get the top song by play count
        top_song = df.groupBy("song").agg(count("song").alias("count")) \
            .orderBy(col("count").desc(), col("song")) \
            .limit(1)

        return gender_counts, level_counts, top_song, user_counts_by_time_zone
    except Exception as e:
        print(f"Error processing table {table_name}: {e}")
        return None, None, None, None

# Loop through tables
for i in range(1, 188):
    table_name = f"group_{i}"
    print(f"\nProcessing table: {table_name}")
    gender_result, level_result, top_song_result, user_counts_result = process_table(table_name)
    
    if gender_result:
        print(f"Gender Breakdown for {table_name}:")
        gender_result.show()
    else:
        print(f"Skipped Gender Breakdown for {table_name} due to errors.")
    
    if level_result:
        print(f"Level Breakdown for {table_name}:")
        level_result.show()
    else:
        print(f"Skipped Level Breakdown for {table_name} due to errors.")
    
    if top_song_result:
        print(f"Top Song for {table_name}:")
        top_song_result.show()
    else:
        print(f"Skipped Top Song for {table_name} due to errors.")
    
    if user_counts_result:
        print(f"Distinct User Counts per Time Zone for {table_name}:")
        user_counts_result.show()
    else:
        print(f"Skipped Distinct User Counts per Time Zone for {table_name} due to errors.")
 
    time.sleep(2)