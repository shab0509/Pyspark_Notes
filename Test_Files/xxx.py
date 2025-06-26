from pyspark import core, RDD
from pyspark.sql import SparkSession
from importlib.resources import files

# Create a SparkSession
sc = SparkSession.builder \
    .appName("BasicPySparkApp") \
    .getOrCreate()

data1=[10,20,30,40,50]
data2=[10,20,30,60,70]

print(data1)
print(type(data1))
print(data2)
print(type(data2))

RDD1 =sc.parallelize(data1)
RDD2 =sc.parallelize(data2)
print(RDD1)
print(type(RDD1))
print(RDD2)
print(type(RDD2))