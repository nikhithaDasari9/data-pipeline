from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, when, from_unixtime,year,rank,month,mean
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType,IntegerType
import logging
from pyspark.sql.window import Window
import pymongo
import sys



# Initialize Spark session
spark = SparkSession.builder \
    .appName("AmazonReviewsAnalysis") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# Set up input path
input_path = "data/All_Amazon_Review.json"

# Define schema to handle duplicate columns
schema = StructType([
    StructField("reviewerID", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("overall", DoubleType(), True),
    StructField("unixReviewTime", IntegerType(), True),
    StructField("reviewText", StringType(), True),
    StructField("scent_col", StringType(), True),        # Rename duplicate columns
    StructField("style_name_col", StringType(), True),   # Rename duplicate columns
    StructField("style_col", StringType(), True)          # Rename duplicate columns
])

# Step 1: Read the dataset with specified schema
df = spark.read.schema(schema).json(input_path)

# Step 2: Data Preprocessing
filtered_df = df.select("asin", "reviewerID", "overall", "unixReviewTime", "reviewText") \
    .filter(col("reviewerID").isNotNull() & col("asin").isNotNull() )


# Extract and transform reviewTime into a valid date format
filtered_df = filtered_df.withColumn("reviewTime", from_unixtime("unixReviewTime", "yyyy-MM-dd"))


# Data validation: Identify and handle outliers
filtered_df = filtered_df.filter((col("overall") >= 1.0) & (col("overall") <= 5.0))

# Data validation: Validate date range
#filtered_df = filtered_df.filter((col("reviewTime") >= "2000-01-01") & (col("reviewTime") <= "2018-12-12"))

# Data validation: Check completeness
filtered_df = filtered_df.filter(~(col("reviewerID").isNull() | col("asin").isNull() ))

# Calculate the mean value of the "overall" column
mean_overall = filtered_df.select(mean("overall")).collect()[0][0]

# Replace null values in the "overall" column with the calculated mean
filtered_df = filtered_df.withColumn("overall", when(col("overall").isNull(), mean_overall).otherwise(col("overall")))



'''
# Data Monitoring and Alerts
logging.basicConfig(
    filename='/app/data_quality.log',  
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
total_records = filtered_df.count()
logging.info(f"Total Records Processed: {total_records}")
'''
print("Number of rows before grouping:", filtered_df.count())

# Step 3: Product-Level Analysis
product_analysis = filtered_df.groupBy("asin").agg(
    avg("overall").alias("average_rating"),
    count("reviewerID").alias("num_reviews"),
    sum(when(col("overall") >= 4, 1).otherwise(0)).alias("num_positive_reviews")
)
print("Number of rows after grouping:", product_analysis.count())

# Step 4: Reviewer-Level Analysis
window_spec = Window.partitionBy("reviewerID")
reviewer_analysis = filtered_df.withColumn("num_reviews_by_reviewer", count("*").over(window_spec)) \
    .select("reviewerID", "num_reviews_by_reviewer") \
    .distinct()

# Save the analysis results to Parquet format
output_path = "output_parquet_path"
product_analysis.write.mode("overwrite").parquet(output_path + "/product_analysis")
reviewer_analysis.write.mode("overwrite").parquet(output_path + "/reviewer_analysis")

# Add year column to the DataFrame
df_with_year = filtered_df.withColumn("reviewYear", year("reviewTime"))

# Calculate the number of reviews written by each reviewer within each year
reviews_per_year_df = df_with_year.groupBy("reviewerID", "reviewYear").count()

# Rank reviewers based on the number of reviews within each year, in descending order
window_spec = Window.partitionBy("reviewYear").orderBy(col("count").desc())
ranked_reviewers_per_year = reviews_per_year_df.withColumn("rank", rank().over(window_spec))

# Show the top reviewers for each year based on review count
top_reviewers_by_year = ranked_reviewers_per_year.where(col("rank") <= 10)

# Display the results
print("Top reviewers for each year based on review count:")
top_reviewers_by_year.show()
top_reviewers_by_year.show()
top_reviewers_by_year.write.mode("overwrite").parquet(output_path + "/top_reviewer_analysis")

# Extract year and month from reviewTime
df = filtered_df.withColumn("reviewYear", year("reviewTime"))
df = df.withColumn("reviewMonth", month("reviewTime"))

# Group by year and month to analyze trends
time_analysis = df.groupBy("reviewYear", "reviewMonth").agg(
    avg("overall").alias("avg_rating"),
    count("reviewerID").alias("num_reviews")
)

# Show the time-based analysis
print("time analysis ::")
time_analysis.show()
time_analysis.write.mode("overwrite").parquet(output_path + "/time_analysis")



# Connect to the MongoDB container instance
client = pymongo.MongoClient("mongodb://mongodb-container:27017/")
db = client["amazon_reviews_db"]

# Define collections for different data models
product_collection = db["products"]
reviewer_collection = db["reviewers"]

# Insert data into NoSQL collections
product_data = product_analysis.collect()

# Print product data before insertion
print("Product Data to be Inserted:")
for doc in product_data:
    print(doc)

product_collection.insert_many(product_data)

reviewer_data = reviewer_analysis.collect()

# Print reviewer data before insertion
print("Reviewer Data to be Inserted:")
for doc in reviewer_data:
    print(doc)

reviewer_collection.insert_many(reviewer_data)

# Query and retrieve products with average_rating greater than or equal to a certain value
selected_products = product_collection.find({"average_rating": {"$gte": 4.0}})

print("Selected Products with High Ratings:")
for product in selected_products:
    print(product)

# Query and retrieve reviewers with a certain number of reviews
selected_reviewers = reviewer_collection.find({"num_reviews_by_reviewer": {"$gte": 10}})

print("Selected Reviewers with High Review Counts:")
for reviewer in selected_reviewers:
    print(reviewer)




# Close the MongoDB connection
client.close()

# Stop the Spark session
spark.stop()



