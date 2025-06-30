from pyspark.sql import SparkSession
from pyspark import SparkContext
import findspark

findspark.init()

spark = SparkSession \
        .builder \
        .appName("Filtering Nulls") \
        .getOrCreate()

sc= spark.sparkContext


line="              spark is for processing             "
words=line.split(" ")
print(words)

words1 =sc.parallelize(words)
'''
words2=words1.filter(lambda x:x!='')
words2.collect()
y=words2.collect()
z=" ".join(y)
print(z)'''
