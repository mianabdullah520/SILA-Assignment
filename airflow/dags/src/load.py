import psycopg2

conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="1234",
            host="localhost"
        )


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
