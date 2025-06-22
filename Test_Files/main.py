 
from pyspark.sql import SparkSession

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("BasicPySparkApp") \
    .getOrCreate()

# Step 2: Create a sample DataFrame
data = [("Alice", 28), ("Bob", 35), ("Cathy", 23)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Step 3: Show the DataFrame
df.show()

# Step 4: Filter rows where Age > 25
filtered_df = df.filter(df.Age > 25)
filtered_df.show()