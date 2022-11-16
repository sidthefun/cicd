from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

df = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("path", 'C:\\Users\\91720\\Downloads\\-201025-223502.dataset1') \
    .load()

df1 = df.toDF("name", "age", "city")


def ageCheck(age):
    print("sid")
    if (age > 18):
        return "Y"
    else:
        return "N"


parseAgeFunction = udf(ageCheck, StringType())

df1.withColumn("adult", parseAgeFunction("age")).show()


