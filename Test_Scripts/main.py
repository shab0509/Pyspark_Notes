from pyarrow import Schema
from pyspark.sql import SparkSession
from pyspark  import RDD

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
        .appName("Create DataFrame Example") \
        .getOrCreate()

# Step 2: Sample data
data1 = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
data2 = [("Anju", 34), ("Balli", 45), ("Rani", 29)]


# Step 3: Define schema (column names)
columns = ["Name", "Age"]

# Step 4: Create DataFrame
df1 = spark.createDataFrame(data1, schema=columns)
df2 = spark.createDataFrame(data2, schema=columns)

# Step 5: Show the DataFrame
df3=df1.union(df2)
df3.show()
