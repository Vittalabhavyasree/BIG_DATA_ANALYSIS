from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum
import pandas as pd
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("Big Data Analysis").getOrCreate()

# Load the dataset
data_path = "dataset.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Show dataset preview
print("Dataset Preview:")
df.show(5)

# Show schema
print("Dataset Schema:")
df.printSchema()

# Handle missing values (fill with median or drop rows if necessary)
df = df.na.drop()

# Basic descriptive statistics
df.describe().show()

# Aggregation: Count of rows per category (if applicable)
if 'category' in df.columns:
    category_count = df.groupBy("category").count()
    print("Category Count:")
    category_count.show()

# Aggregation: Average of numerical columns
numerical_cols = [field.name for field in df.schema.fields if field.dataType.simpleString() in ['int', 'double']]
if numerical_cols:
    avg_values = df.select([avg(col(c)).alias(f"avg_{c}") for c in numerical_cols])
    print("Average Values:")
    avg_values.show()

# Ensure output directory exists
output_dir = "/mnt/data/output"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "big_data_analysis_output.csv")

# Save insights to CSV
df.limit(1000).toPandas().to_csv(output_path, index=False)
print(f"Insights saved to: {output_path}")

# Stop Spark Session
spark.stop()
