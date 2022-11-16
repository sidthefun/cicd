from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDf = spark.read.format("csv") \
    .option("header",True) \
    .option("inferSchema",True) \
    .option("path" , 'C:\\Users\\91720\\Downloads\\order_data.csv') \
    .load()



orderDf.createOrReplaceTempView("dftable")

spark.sql("select InvoiceNo,CustomerID from dftable  where Quantity >25").show()


