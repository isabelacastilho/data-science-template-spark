from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import sql


config = SparkConf().setMaster("local[8]").set("spark.executor.memory","2g")
sc = SparkContext(conf = config)
sqlContext = sql.SQLContext(sc)