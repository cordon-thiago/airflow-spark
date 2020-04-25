from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
master = "spark://spark:7077"
conf = SparkConf().setAppName("First App").setMaster(master)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# from pyspark import SparkContext
# sc = SparkContext("local", "First App")

logFile = "airflow/airflow.cfg"
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: {}, lines with b: {}".format(numAs, numBs))
