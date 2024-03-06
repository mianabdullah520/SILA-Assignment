from src.ingest import ingest_data
from src.transform import drop_columns
from src.transform import handle_missing_values
from src.transform import handle_outliers
from src.transform import normalize_text
from src.analyze import top_10_products_analysis

dataset_path = "data/Digital_Music.json"

# Call the ingest_data function to get the DataFrame
df = ingest_data(dataset_path)


# drop irrelevant columns
df=drop_columns(df)
# handle missing values
df=handle_missing_values(df)
# handle outliers
df=handle_outliers(df)

# Normalize text columns
df = normalize_text(df)


# calling function to get the analysis of top 10 products
top_10_products_analysis(df)


