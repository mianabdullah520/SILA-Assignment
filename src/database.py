import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine,text
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import mean
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.sql.functions import lower, regexp_replace

conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="1234",
            host="localhost"
        )


import pandas as pd

def create_table():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="1234",
            host="localhost"
        )

        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        # Define the schema for the table
        table_name = "reviews"
        schema = (
            ("asin", "VARCHAR(255)"),
            ("reviewerID", "VARCHAR(255)"),
            ("overall", "NUMERIC"),
            ("reviewText", "TEXT"),
            ("summary", "TEXT"),
            ("unixReviewTime", "BIGINT")
        )

        # Construct the CREATE TABLE SQL statement
        columns = ', '.join([f"{name} {data_type}" for name, data_type in schema])
        create_table_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY (asin, reviewerID))").format(
            sql.Identifier(table_name),
            sql.SQL(columns)
        )

        # Execute the CREATE TABLE statement
        cursor.execute(create_table_sql)

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        print("Table created successfully!")

    except psycopg2.Error as e:
        print("Error creating table:", e)


def insert_data(df):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="1234",
            host="localhost"
        )

        # Create a SQLAlchemy engine
        engine = create_engine('postgresql+psycopg2://postgres:1234@localhost/postgres')

        # Convert Spark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()
        column_names = pandas_df.columns

        # Print the column names
        print("Column Names:", column_names)

        # Insert data into PostgreSQL table
        pandas_df.to_sql('reviews', engine, if_exists='append',index=False)

        print("Data inserted successfully!")


    except psycopg2.Error as e:
        print("Error inserting data:", e)


def create_table_and_insert_data(df1, table_name):
    # Define the SQL create table query based on the dataframe columns
    df=df1.toPandas()
    # Define the SQL create table query based on the dataframe columns
    columns = ', '.join(f'"{col}" TEXT' for col in df.columns)
    create_table_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns})'

    # Create the table
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into the table
    dataframe_columns = ', '.join(f'"{col}"' for col in df.columns)
    insert_query = f'INSERT INTO "{table_name}" ({dataframe_columns}) VALUES ({", ".join(["%s"] * len(df.columns))})'
    values = [tuple(row) for row in df.to_numpy()]
    cursor.executemany(insert_query, values)
    conn.commit()
    cursor.close()

# Call the function to insert data into the table
#insert_data(df)








# Call the function to create the table
#create_table()
