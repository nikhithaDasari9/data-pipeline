# Data Pipeline Application

This repository contains a data pipeline application developed using Apache Spark and PySpark. The application processes and analyzes a large dataset of Amazon reviews.

## Table of Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Usage](#usage)
- [Configuration](#configuration)
- [Output](#output)
- [Dockerization](#dockerization)

## Overview

The data pipeline application reads an Amazon reviews dataset in JSON format, performs data processing tasks such as  aggregations, and summarizations, and saves the transformed data in Parquet format. The application also includes data validation and monitoring mechanisms to ensure data quality and governance.

## Requirements

- Apache Spark
- PySpark
- Python 3.x
- Docker (optional)

## Usage

## Clone this repository to your local machine:

   ```bash
   git clone https://github.com/nikhithaDasari9/data-pipeline.git
   ```
Download the Amazon reviews dataset (2018) in JSON format and place it in the repository directory.

## Install the required Python packages:

pip install -r requirements.txt

## Run the data pipeline script:

python data_pipeline.py

This script reads the input JSON dataset, performs data processing tasks, and saves the results in Parquet format.

#configuration

Modify the input_path variable in the data_pipeline.py script to point to the location of your input JSON dataset.
Output paths and other configurations can be adjusted in the data_pipeline.py script.

## Output
The transformed data is saved in Parquet format. The output is stored in the output_parquet_path directory. You can configure the output paths in the data_pipeline.py script.
## MongoDB Integration

The data pipeline application also integrates with MongoDB for data storage and retrieval. Here's how you can perform common tasks using MongoDB within the application:

### Data Insertion

The application uses PyMongo to insert transformed data into MongoDB collections. Sample code to insert data into MongoDB is provided in the `data_pipeline.py` script. Make sure to replace the MongoDB connection string and provide the appropriate data for insertion.

```python
import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb-container:27017/")

# Create or get the database
db = client["amazon_reviews_db"]

# Create collections
product_collection = db["products"]
reviewer_collection = db["reviewers"]

# Insert data into collections
product_collection.insert_many(product_data)
reviewer_collection.insert_many(reviewer_data)

# Print success message
print("Sample data inserted into MongoDB collections.")

# Find all products with an average rating greater than 4.5
high_rated_products = product_collection.find({"average_rating": {"$gt": 4.5}})

# Iterate through the results
for product in high_rated_products:
    print(product)

Data Selection
You can use PyMongo to query and retrieve data from MongoDB collections. Here's an example of how to retrieve data based on a condition:
# Find all products with an average rating greater than 4.5
high_rated_products = product_collection.find({"average_rating": {"$gt": 4.5}})

# Iterate through the results
for product in high_rated_products:
    print(product)

Data Update
You can also update documents in MongoDB collections using PyMongo. Here's an example of how to update the average rating of a product:

# Update the average rating of a product
product_collection.update_one(
    {"asin": "B000123XYZ"},
    {"$set": {"average_rating": 4.8}}
)

print("Product updated successfully.")

```


Dockerization (Optional)
To containerize the data pipeline application using Docker:

Build the Docker image:

docker build -t data-pipeline .

Run the Docker container:

docker run --name data-pipeline-container -v /path/on/host:/app/output_parquet_path data-pipeline
Replace /path/on/host with the path on your host system where you want to save the output data.


# When running the Docker container, use the -v option to mount a local directory
# containing the input data into the container. This allows the data pipeline script
# to access and process the data from the specified local path. Replace
# "/path/on/host/your_data_directory" with the actual path on your host system where
# the data is located, and "/path/in/container/data" with the path inside the
# container where you want to access the data.
