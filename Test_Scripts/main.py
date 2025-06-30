from pyspark.sql import SparkSession
# Step 1: Initialize SparkSession
spark = SparkSession.builder \
        .appName("Create DataFrame Example") \
        .getOrCreate()

columns = ["Name", "Age"]
data1 = spark.createDataFrame([("Alice", 34), ("Bob", 45), ("Catherine", 29)],schema=columns)
data2 = spark.createDataFrame([("Anju", 34), ("Balli", 45), ("Rani", 29)],schema=columns)

res = data1.collect()
print(res)
#data2.collect()


