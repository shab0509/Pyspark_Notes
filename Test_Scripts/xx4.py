from pyspark.sql import SparkSession
from pyspark import find_spark_home
from spark import createDataFrame

spark = SparkSession \
        .builder \
        .appName("Group+ Agg") \
        .getOrCreate()



r1= spark.read.text( "/Users/shabivictor/Shabi_Personal/GITHUB/Pyspark_Notes/Data_Files/emp.txt")
r1.show()
r2 = r1.map(lambda x: x.split(","))
r2.show()
#r3 = r2.map(lambda x: (x[4], int(x[2])))
#r3.collect()
#res = r3.reduceByKey(lambda x, y: x + y)
#res.collect()



