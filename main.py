from src.ingest import ingest_data
from src.transform import drop_columns
from src.transform import handle_missing_values
from src.transform import handle_outliers
from src.transform import normalize_text
from src.database import create_table_and_insert_data
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






# due to limited memory issue of local machine had to split the data set for writting into DB.
weights = [0.5, 0.5]
# Split the DataFrame into two parts
split_dataframes = df.randomSplit(weights)

# Now 'split_dataframes' is an array of two DataFrames, each representing one part
# You can access individual parts like this
part1 = split_dataframes[0]
part2 = split_dataframes[1]



#create_table_and_insert_data(part1,'reviews')
create_table_and_insert_data(part2,'reviews')

