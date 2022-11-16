from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext

my_conf = SparkConf()
my_conf.set("spark.app.name", "MovieDB")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

rattings_rdd = spark.read.format("csv") \
    .option("header",True) \
    .option("inferSchema",True) \
    .option("path",'C:\\Users\\91720\\Downloads\\ratings.dat') \
    .load()

mapped_rdd = rattings_rdd.rdd.map(lambda x: (x.split("::")[1],x.split("::")[2]))

mapped_rdd1= mapped_rdd.toDF()

print(mapped_rdd1)