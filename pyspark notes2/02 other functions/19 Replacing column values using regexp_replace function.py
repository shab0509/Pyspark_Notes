Pyspark SQL functions -Replacing Column values in a DataFrame

1)regexp_replace()
2)translate()
3)overlay()

These are used
 i)for replacing a portion of a string with another string
ii)for replacing all columns
iii)Change values Conditionally
iv)replace values from a Python dictionary
v)replace column value  from another DataFrame

1)Replacing String column values

>>> address=[(1,"24 beach Rd","chennai"),
...          (2,"308 Gandhi st","BAN"),
...          (3,"406 Temple Rd","HYD")]

>>> df=spark.createDataFrame(address,["id","addr","city"])
>>> df.show()
+---+-------------+-------+
| id|         addr|   city|
+---+-------------+-------+
|  1|  24 beach Rd|chennai|
|  2|308 Gandhi st|    BAN|
|  3|406 Temple Rd|    HYD|
+---+-------------+-------+

TasK: Replace Rd with Road  using regexp_replace()
>>> from pyspark.sql.functions import regexp_replace
>>> df2=df.withColumn('addr',regexp_replace('addr','Rd','Road'))
>>>df2.show()
+---+---------------+-------+
| id|           addr|   city|
+---+---------------+-------+
|  1|  24 beach Road|chennai|
|  2|  308 Gandhi st|    BAN|
|  3|406 Temple Road|    HYD|
+---+---------------+-------+

-----------------------------------------------------------------------------------------------
2)Replacing Column Values Conditionally using pyspark sql functions like 
1)when() 
2)otherwise()

>>> from pyspark.sql.functions import when
>>> df.withColumn('addr',when(df.addr.endswith('Rd'),regexp_replace(df.addr,'Rd','Road')) \
...  .when(df.addr.endswith('st'),regexp_replace(df.addr,'st','street')) \
...  .otherwise(df.addr)).show()
+---+-----------------+-------+
| id|             addr|   city|
+---+-----------------+-------+
|  1|    24 beach Road|chennai|
|  2|308 Gandhi street|    BAN|
|  3|  406 Temple Road|    HYD|
+---+-----------------+-------+
-----------------------------------------------------------------------------------------------
3) Replace Column Value with Dictionary(map)
>>> citydict={'chennai':'chennai','BAN':'Banglore','HYD':'Hyderabad'}
>>> df2=df.rdd.map(lambda x:(x.id,x.addr,citydict[x.city])).toDF(["id","addr","city"])
>>> df2.show()
+---+-------------+---------+
| id|         addr|     city|
+---+-------------+---------+
|  1|  24 beach Rd|  chennai|
|  2|308 Gandhi st| Banglore|
|  3|406 Temple Rd|Hyderabad|
+---+-------------+---------+

-----------------------------------------------------------------------------------------------


























