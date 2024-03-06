import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="1234",
    host="localhost"
)

def perform_data_migration():
    try:
        # Open a cursor to perform database operations
        cursor = conn.cursor()

        # Execute SQL query to update records with sentiment analysis results
        update_query = """
        UPDATE reviews
        SET sentiment_analysis_result = 'positive'
        WHERE reviewText LIKE '%good%';
        """
        cursor.execute(update_query)

        # Commit the transaction
        conn.commit()
        print("Data migration completed successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error during data migration:", error)
        conn.rollback()

    finally:
        # Close communication with the database
        cursor.close()
        conn.close()