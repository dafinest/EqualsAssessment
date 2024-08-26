from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Real-Time Transaction Processing with PostgreSQL") \
    .config("spark.jars", "/path/to/postgresql-42.2.23.jar") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True)
])

# Read from the simulated source (socket on port 9999)
transaction_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON strings into structured data
parsed_df = transaction_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Process the data 
aggregated_df = parsed_df.groupBy("transaction_type").sum("transaction_amount").alias("total_amount")

# Define the JDBC connection properties
jdbc_url = "jdbc:postgresql://<hostname>:<port>/<database>" #put hostname, port and database
db_properties = {
    "user": "<username>",       #put username
    "password": "<password>",   #put password
    "driver": "org.postgresql.Driver"
}

# Define a function to write the data to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write.jdbc(
        url=jdbc_url,
        table="processed_transactions",
        mode="append",
        properties=db_properties
    )

# Write the data to PostgreSQL in real-time
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_postgres) \
    .start()

# Wait for the termination signal
query.awaitTermination()
