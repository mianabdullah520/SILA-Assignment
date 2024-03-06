from pyspark.sql import DataFrame
from pyspark.sql.functions import mean
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.sql.functions import lower, regexp_replace



def drop_columns(df: DataFrame) -> DataFrame:
    # Define columns to drop
    columns_to_drop = ['reviewerName', 'style', 'reviewTime', 'image', 'verified', 'vote']
    # Drop the specified columns from the DataFrame
    df = df.drop(*columns_to_drop)
    return df


def handle_missing_values(df):
    # Fill missing values in 'reviewText' and 'summary' columns with empty strings
    df = df.fillna({'reviewText': '', 'summary': ''})

    # Fill missing values in 'overall' column with mean value
    overall_mean = df.select(mean('overall')).collect()[0][0]
    df = df.fillna({'overall': overall_mean})

    # Fill missing values in 'unixReviewTime' column with None
    df = df.withColumn('unixReviewTime', when(df['unixReviewTime'].isNull(), None).otherwise(df['unixReviewTime']))

    # Fill missing values in 'reviewerID' column with None
    df = df.withColumn('reviewerID', when(df['reviewerID'].isNull(), None).otherwise(df['reviewerID']))

    return df




def handle_outliers(df):
    # Filter out rows where 'overall' column values are within the range [1.0, 5.0]
    temp_df = df.filter((col('overall') >= 1.0) & (col('overall') <= 5.0))
    return temp_df




def normalize_text(df):
    # Apply text normalization to reviewText column
    df = df.withColumn("reviewText", lower(regexp_replace("reviewText", "[^a-zA-Z0-9\\s]", "")))
    # Apply text normalization to summary column
    df = df.withColumn("summary", lower(regexp_replace("summary", "[^a-zA-Z0-9\\s]", "")))
    return df
