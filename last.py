# import time
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, coalesce, lit, count

# # Initialize SparkSession
# spark = SparkSession \
#     .builder \
#     .appName('test') \
#     .master('local[*]') \
#     .config("spark.driver.extraClassPath", "/Users/mnawar/Desktop/passion/CaDence/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar") \
#     .getOrCreate()

# # Function to process a table and return the gender, level counts, and top song
# def process_table(table_name):
#     # Read data from the table
#     df = spark.read.format("jdbc") \
#         .option("url", "jdbc:mysql://localhost/cadenceDB") \
#         .option("driver", "com.mysql.cj.jdbc.Driver") \
#         .option("dbtable", table_name) \
#         .option("user", "mnawar") \
#         .option("password", "thisistheway") \
#         .load()

#     # Get distinct userId, gender, and level
#     unique_users_gender_level = df.select("userId", "gender", "level").distinct()

#     # Group by gender and count unique userId per gender
#     gender_counts = unique_users_gender_level.groupBy("gender").count()

#     # Group by level and count unique userId per level
#     level_counts = unique_users_gender_level.groupBy("level").count()

#     # Pivot the gender counts to have columns for Male and Female, with 0 for missing genders
#     gender_pivoted = gender_counts.groupBy().pivot("gender").agg({"count": "sum"}).fillna(0)

#     # Safely handle missing 'M' and 'F' columns by checking for their existence
#     if 'M' in gender_pivoted.columns:
#         gender_pivoted = gender_pivoted.withColumn("male", coalesce(col("M"), lit(0)))
#     else:
#         gender_pivoted = gender_pivoted.withColumn("male", lit(0))

#     if 'F' in gender_pivoted.columns:
#         gender_pivoted = gender_pivoted.withColumn("female", coalesce(col("F"), lit(0)))
#     else:
#         gender_pivoted = gender_pivoted.withColumn("female", lit(0))

#     # Drop the original 'M' and 'F' columns if they exist
#     gender_pivoted = gender_pivoted.drop("M", "F")

#     # Pivot the level counts to have columns for Paid and Free
#     level_pivoted = level_counts.groupBy().pivot("level").agg({"count": "sum"}).fillna(0)

#     # Manually ensure the presence of 'paid' and 'free' columns with default values (0)
#     if 'paid' not in level_pivoted.columns:
#         level_pivoted = level_pivoted.withColumn("paid", lit(0))
#     if 'free' not in level_pivoted.columns:
#         level_pivoted = level_pivoted.withColumn("free", lit(0))

#     # Get the top song by count of listens
#     top_song = df.groupBy("song").agg(count("song").alias("count")) \
#         .orderBy(col("count").desc(), col("song")) \
#         .limit(1)

#     return gender_pivoted, level_pivoted, top_song

# # Process all tables from group_1 to group_200 with a 2-second delay between each
# for i in range(186, 188):
#     table_name = f"group_{i}"

#     print(f"\nProcessing table: {table_name}")
    
#     # Process the gender, level counts, and top song
#     gender_result, level_result, top_song_result = process_table(table_name)
    
#     print(f"Gender Breakdown for {table_name}:")
#     gender_result.show()
    
#     print(f"Level Breakdown for {table_name}:")
#     level_result.show()

#     print(f"Top Song for {table_name}:")
#     top_song_result.show()

#     # Add a 2-second delay between processing each table
#     time.sleep(2)

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, count, countDistinct, udf
from pyspark.sql.types import StringType
# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName('Table Processing') \
    .master('local[*]') \
    .config("spark.driver.extraClassPath", "/Users/mnawar/Desktop/passion/CaDence/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar") \
    .getOrCreate()
# Dictionary mapping state abbreviations to full state names
state_abbr_to_full = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado",
    "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana",
    "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"
}
# Dictionary mapping states to their respective time zones
state_time_zone_map = {
    "Alabama": "Central Time Zone (CT)",
    "Alaska": "Pacific Time Zone (PT)",
    "Arizona": "Mountain Time Zone (MT)",
    "Arkansas": "Central Time Zone (CT)",
    "California": "Pacific Time Zone (PT)",
    "Colorado": "Mountain Time Zone (MT)",
    "Connecticut": "Eastern Time Zone (ET)",
    "Delaware": "Eastern Time Zone (ET)",
    "Florida": "Eastern Time Zone (ET)",
    "Georgia": "Eastern Time Zone (ET)",
    "Hawaii": "Hawaii-Aleutian Time Zone (HAT)",
    "Idaho": "Mountain Time Zone (MT)",
    "Illinois": "Central Time Zone (CT)",
    "Indiana": "Eastern Time Zone (ET) and Central Time Zone (CT)",
    "Iowa": "Central Time Zone (CT)",
    "Kansas": "Central Time Zone (CT)",
    "Kentucky": "Eastern Time Zone (ET) and Central Time Zone (CT)",
    "Louisiana": "Central Time Zone (CT)",
    "Maine": "Eastern Time Zone (ET)",
    "Maryland": "Eastern Time Zone (ET)",
    "Massachusetts": "Eastern Time Zone (ET)",
    "Michigan": "Eastern Time Zone (ET)",
    "Minnesota": "Central Time Zone (CT)",
    "Mississippi": "Central Time Zone (CT)",
    "Missouri": "Central Time Zone (CT)",
    "Montana": "Mountain Time Zone (MT)",
    "Nebraska": "Central Time Zone (CT)",
    "Nevada": "Pacific Time Zone (PT)",
    "New Hampshire": "Eastern Time Zone (ET)",
    "New Jersey": "Eastern Time Zone (ET)",
    "New Mexico": "Mountain Time Zone (MT)",
    "New York": "Eastern Time Zone (ET)",
    "North Carolina": "Eastern Time Zone (ET)",
    "North Dakota": "Central Time Zone (CT)",
    "Ohio": "Eastern Time Zone (ET)",
    "Oklahoma": "Central Time Zone (CT)",
    "Oregon": "Pacific Time Zone (PT)",
    "Pennsylvania": "Eastern Time Zone (ET)",
    "Rhode Island": "Eastern Time Zone (ET)",
    "South Carolina": "Eastern Time Zone (ET)",
    "South Dakota": "Central Time Zone (CT)",
    "Tennessee": "Eastern Time Zone (ET) and Central Time Zone (CT)",
    "Texas": "Central Time Zone (CT)",
    "Utah": "Mountain Time Zone (MT)",
    "Vermont": "Eastern Time Zone (ET)",
    "Virginia": "Eastern Time Zone (ET)",
    "Washington": "Pacific Time Zone (PT)",
    "West Virginia": "Eastern Time Zone (ET)",
    "Wisconsin": "Central Time Zone (CT)",
    "Wyoming": "Mountain Time Zone (MT)"
}
# UDF to assign time zone based on state
def get_time_zone(state):
    # Convert abbreviation to full state name if necessary
    full_state_name = state_abbr_to_full.get(state, state)  # Default to the state if already full name
    # If the state is found in the map, return the time zone
    if full_state_name in state_time_zone_map:
        return state_time_zone_map.get(full_state_name)
    # If the state is not found, return an error message
    return f"State '{full_state_name}' not found. Please check the state abbreviation."
# Register UDF in Spark
get_time_zone_udf = udf(get_time_zone, StringType())
def process_table(table_name):
    """Process a single table to calculate gender, level counts, top song, time zone, and user counts in time zones."""
    try:
        # Read data from the table
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:mysql://localhost/cadenceDB") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", "mnawar") \
            .option("password", "thisistheway") \
            .load()
        # Add a time zone column based on the state column
        df = df.withColumn("time_zone", get_time_zone_udf(col("state")))
        # Get the distinct count of users per time zone
        user_counts_by_time_zone = df.groupBy("time_zone").agg(countDistinct("userId").alias("distinct_user_count"))
        # Get distinct userId, gender, and level
        unique_users_gender_level = df.select("userId", "gender", "level").distinct()
        # Calculate gender breakdown
        # Get distinct userId, gender, and level
        unique_users_gender_level = df.select("userId", "gender", "level").distinct()
        # Group by gender and count unique userId per gender
        gender_counts = unique_users_gender_level.groupBy("gender").count()
        # Group by level and count unique userId per level
        level_counts = unique_users_gender_level.groupBy("level").count()
        # Pivot the gender counts to have columns for Male and Female, with 0 for missing genders
        gender_pivoted = gender_counts.groupBy().pivot("gender").agg({"count": "sum"}).fillna(0)
        # Safely handle missing 'M' and 'F' columns by checking for their existence
        if 'M' in gender_pivoted.columns:
            gender_pivoted = gender_pivoted.withColumn("male", coalesce(col("M"), lit(0)))
        else:
            gender_pivoted = gender_pivoted.withColumn("male", lit(0))
        if 'F' in gender_pivoted.columns:
            gender_pivoted = gender_pivoted.withColumn("female", coalesce(col("F"), lit(0)))
        else:
            gender_pivoted = gender_pivoted.withColumn("female", lit(0))
        # Drop the original 'M' and 'F' columns if they exist
        gender_pivoted = gender_pivoted.drop("M", "F")
        # Calculate level breakdown
        level_counts = unique_users_gender_level.groupBy("level").count()
        level_pivoted = level_counts.groupBy().pivot("level").agg({"count": "sum"}).fillna(0)
        if 'paid' not in level_pivoted.columns:
            level_pivoted = level_pivoted.withColumn("paid", lit(0))
        if 'free' not in level_pivoted.columns:
            level_pivoted = level_pivoted.withColumn("free", lit(0))
        # Get the top song by play count
        top_song = df.groupBy("song").agg(count("song").alias("count")) \
            .orderBy(col("count").desc(), col("song")) \
            .limit(1)
        return gender_pivoted, level_pivoted, top_song, user_counts_by_time_zone
    except Exception as e:
        print(f"Error processing table {table_name}: {e}")
        return None, None, None, None

for i in range(186, 188):
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