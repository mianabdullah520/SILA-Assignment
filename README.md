# SILA-Assignment
Note: All the Commits are without data sets and installed packages and libraries since those files exceeds giga-bites in size and github does now allow pushing such large files to github repos.


**Introduction**
The Data Engineering Assignment aims to ingest, transform, analyze, and load a dataset into a database system. The project involves using technologies such as Apache Spark, PostgreSQL, Apache Airflow, and Docker to automate and streamline the data engineering processes.

**Objectives**
Ingest a dataset into the system.
Transform the dataset to prepare it for analysis.
Analyze the dataset to derive insights.
Load the transformed data into a PostgreSQL database.
Automate the data engineering workflow using Apache Airflow.
Deploy the project using Docker for scalability and portability.


**Scope**
The project focuses on handling a dataset of reviews, performing data transformation and analysis, and storing the results in a relational database. The technologies used include Apache Spark for data processing, PostgreSQL for data storage, Apache Airflow for workflow automation, and Docker for containerization.


**PostgreSQL Schema**
The PostgreSQL schema defines the structure of the database tables where the transformed data is stored. The schema is designed to efficiently store and retrieve relevant information for analysis.

**Table: reviews**
Columns:
asin (TEXT): Represents the product ID associated with each review. The 'asin' column serves as the primary key for identifying products.
overall (TEXT): Denotes the overall rating assigned to the product by the reviewer. The 'overall' column captures the sentiment of the review, ranging from 1.0 to 5.0.
reviewText (TEXT): Contains the detailed review text provided by the reviewer. The 'reviewText' column stores the textual content of the review, which can be analyzed for sentiment analysis or keyword extraction.
reviewerID (TEXT): Represents the unique identifier of the reviewer who submitted the review. The 'reviewerID' column enables tracking and analysis of individual reviewer behavior.
summary (TEXT): Provides a concise summary or title of the review. The 'summary' column complements the 'reviewText' by capturing key points or sentiments expressed in the review.
unixReviewTime (TEXT): Indicates the time when the review was submitted, represented in Unix timestamp format. The 'unixReviewTime' column facilitates temporal analysis and trend identification over time.




**Explanation:**
Primary Key: The composite key consisting of 'asin' and 'reviewerID' ensures uniqueness and data integrity, as each product can have multiple reviews from different reviewers.
Data Types: Text data types are chosen for flexibility in storing variable-length textual content.
Normalization: The schema is designed to reduce data redundancy and ensure efficient storage by normalizing textual content and separating attributes into distinct columns.
Scalability: The schema can accommodate a large volume of reviews and support future scalability by leveraging PostgreSQL's indexing and partitioning capabilities.



**Why These Columns?**
asin: Essential for identifying and grouping reviews by product, facilitating product-level analysis and comparison.
overall: Captures the sentiment or rating associated with each review, enabling sentiment analysis and sentiment-based filtering.
reviewText: Stores the detailed textual content of the review, allowing for text mining, sentiment analysis, and natural language processing (NLP) tasks.
reviewerID: Enables tracking and profiling of individual reviewers, facilitating user-level analysis and personalized recommendations.
summary: Provides a concise representation of the review content, useful for summarization, keyword extraction, and sentiment analysis.
unixReviewTime: Records the timestamp of review submission, supporting temporal analysis and trend detection over time.




**Data Ingestion**
Data ingestion involves importing data from an external source into the system for further processing.

**Overview**
The dataset used for this project is obtained from a digital music platform and is in JSON format.

**Implementation**
The ingest_data function in ingest.py utilizes Apache Spark to read the JSON data and convert it into a DataFrame.

**Data Transformation**
Data transformation encompasses cleaning, filtering, and normalizing the dataset to prepare it for analysis.

**Overview**
Data transformation is necessary to ensure data quality and consistency for accurate analysis.

**Implementation**
The drop_columns function in transform.py removes irrelevant columns from the DataFrame.
The handle_missing_values function handles missing values by filling them with appropriate defaults.
The handle_outliers function filters out rows with outlier values in the 'overall' column.
The normalize_text function normalizes text data by converting it to lowercase and removing special characters.


**Data Loading**
Data loading involves storing the transformed data into a database for storage and retrieval.

**Overview**
Loading data into a database enables efficient querying and analysis of large datasets.

**Implementation**
The create_table_and_insert_data function in load.py creates a table in a PostgreSQL database and inserts the transformed DataFrame into the table.

**Data Analysis**
Data analysis entails extracting insights and trends from the dataset to support decision-making.

**Overview**
Analyzing the dataset helps identify patterns, correlations, and anomalies.

**Implementation**
The top_10_products_analysis function in analyze.py analyzes the dataset to identify the top 10 most reviewed products and their average ratings.

**Apache Airflow Integration**
Apache Airflow is used to automate the data engineering workflow, including data ingestion, transformation, analysis, and loading.

**Overview**
Apache Airflow provides a platform for orchestrating and scheduling data pipeline tasks.

**Implementation**
A DAG (Directed Acyclic Graph) is created in Apache Airflow to define the workflow tasks and dependencies. The DAG includes tasks for data ingestion, transformation, analysis, and loading.

**Data Migration and Schema Changes**
Data migration involves updating the database schema to accommodate changes in data structure or requirements.

**Overview**
Schema changes may include adding or modifying columns, tables, or constraints.

**Implementation**
A script is implemented to handle schema changes in the PostgreSQL database. The script ensures data integrity by preserving existing data during schema evolution.

**Docker Deployment**
Docker is used to containerize the project for easy deployment and scalability.

**Overview**
Docker containers encapsulate the project dependencies and configurations, making it portable across different environments.

**Implementation**
The project is packaged into Docker containers using a Dockerfile and docker-compose.yml. Docker Compose is used to define and manage the project's multi-container environment.

**Running the Project**
Follow these steps to run the project:

Build Docker Image: Run docker build -t data_engineering_project . in the project directory to build the Docker image.

Run Docker Compose: Execute docker-compose up to start the Docker containers.

Access Airflow Dashboard: Once the containers are up and running, access the Airflow dashboard by navigating to localhost:8080 in your web browser.

Run DAG: In the Airflow dashboard, locate the DAG for the project and trigger it to start the ETL process. The DAG will automate data ingestion, transformation, analysis, and loading tasks.

Manual/Scheduled Execution: You can manually execute the DAG or schedule it to run at specific intervals, depending on your requirements.
