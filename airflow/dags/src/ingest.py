# ingestion.py
from pyspark.sql import SparkSession
def ingest_data(dataset_path):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Data Ingestion") \
        .getOrCreate()

    # Read JSON data into a DataFrame
    df = spark.read.json(dataset_path)

    # Return the DataFrame
    return df

