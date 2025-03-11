import time
import os
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, udf, count
from pyspark.sql.types import StringType

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName('Table Processing') \
    .master('local[*]') \
    .config("spark.driver.extraClassPath", "/path/to/mysql-connector-j-9.1.0.jar") \
    .getOrCreate()

# Database connection configuration for cadenceDB2
db_config2 = {
    'host': 'localhost',
    'user': 'mnawar',
    'password': os.getenv('PASS'),
    'database': 'cadenceDB2'  # Target database for results
}

# Function to establish a connection to the new database
def get_db_connection():
    return mysql.connector.connect(**db_config2)

# Function to create necessary tables for storing results in cadenceDB2
def create_results_tables(table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Create table for gender counts
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{table_name}_gender_counts` (
                gender VARCHAR(10),
                count INT
            )
        """)
        
    except mysql.connector.Error as e:
        print(f"Error creating tables in cadenceDB2 for {table_name}: {e}")
    
    finally:
        cursor.close()
        conn.close()
