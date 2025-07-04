from pyspark import core, RDD
from pyspark.sql import SparkSession

# Create a SparkSession
'''
spark = SparkSession.builder \
        .appName("BasicPySparkApp") \
        .config("spark.driver.bindAddress", "localhost") \
        .getOrCreate()
'''
spark = SparkSession.builder \
        .appName("BasicPySparkApp") \
        .getOrCreate()

sc =spark.sparkContext

data1=[10,20,30,40,50]
data2=[10,20,30,60,70]

print(data1)
print(type(data1))
print(data2)
print(type(data2))

RDD1 =sc.parallelize(data1)
RDD2 =spark.createDataFrame(data2)
print(RDD1.show())
print(type(RDD1))
print(RDD2.show())
print(type(RDD2))
