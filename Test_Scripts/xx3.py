from pyarrow.jvm import schema
from pyspark.pandas.typedef import spark_type_to_pandas_dtype
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
        .builder \
        .appName("Read text file") \
        .getOrCreate()

#r1=sc.parallelize("/Users/shabivictor/Shabi_Personal/GITHUB/Pyspark_Notes/Data_Files/emp.txt")

r1=spark.read.text("/Users/shabivictor/Shabi_Personal/GITHUB/Pyspark_Notes/Data_Files/emp.txt")
print(type(r1))

print("Splitting the values based on comma")

#r2 = r1.map(lambda x:x.split(","))
r2 =r1.mapInArrow(lambda  x: x)
print(type(r2))
r2.collect()

'''
r3=r2.map(lambda x:(x[1],int(x[2])))
r3.collect()

r4=r3.filter(lambda x:x[1]>20000)
r4.collect()
#[(u'Sony', 30000), (u'Sita', 40000), (u'James', 50000)]
'''



