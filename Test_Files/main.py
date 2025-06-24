from pyspark.sql import SparkSession
import os
import sys

os.environ['SPARK_HOME'] ="/Users/shabivictor/Shabi_Personal/Spark/spark-4.0.0-bin-hadoop3"
sys.path.append("/Users/shabivictor/Shabi_Personal/Spark/spark-4.0.0-bin-hadoop3/bin")
# Step 1: Initialize SparkSession
sc = SparkSession.builder \
    .master("local") \
    .appName("Pyspark_Shabi") \
    .getOrCreate()

r1 = sc.parallelize([10, 20, 30, 40, 50])
r2 = sc.parallelize([10, 20, 30, 60, 70])

res = r1.union(r2)
res2 = res.collect()
print(res2)
