
import spark
from pyspark.sql import SparkSession

# Create a SparkSession
sc = SparkSession.builder \
    .appName("BasicPySparkApp") \
    .getOrCreate()

# created
a=[10,20,30,40,50]
b=[10,20,30,60,70]

columns = ["Age"]
r3= spark.createDataFrame(a,schema=columns)
r4= spark.createDataFrame(b, schema=columns)

res1= r3.union(r4)
res2= res1.collect()
print(res2)
