from pyspark.sql.functions import desc,col

def top_10_products_analysis(df):
    # Group by product ID and count the number of reviews for each product
    product_reviews = df.groupBy("asin").count()

    # Sort the products by the number of reviews in descending order
    top_10_products = product_reviews.orderBy(desc("count")).limit(10)

    # Calculate the average rating for each of the top 10 products
    top_10_products_avg_rating = top_10_products.join(df, "asin") \
        .groupBy("asin", "count").avg("overall") \
        .withColumnRenamed("avg(overall)", "average_rating")

    sorted_df = top_10_products_avg_rating.orderBy(top_10_products_avg_rating["count"].desc())

    # Show the sorted DataFrame
    sorted_df.show()
