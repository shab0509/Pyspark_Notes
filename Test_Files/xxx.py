from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BasicPySparkApp") \
    .getOrCreate()

# Create a sample DataFrame
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
columns = ["Name", "ID"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()