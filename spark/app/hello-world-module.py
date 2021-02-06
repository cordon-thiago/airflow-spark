import sys
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StringType
from modules import moduleExample

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

####################################
# Parameters
####################################
csv_file = sys.argv[1]

####################################
# Read CSV Data
####################################
print("######################################")
print("READING CSV FILE")
print("######################################")

df_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(csv_file)
)

print("######################################")
print("SAMPLING CSV DATA")

# Applying the Spark function
df_csv_sample = moduleExample.pysparkFunctions.sample_df(df_csv, 0.1)

print("Number of rows after sampling: {}".format(df_csv_sample.count())) 
print("######################################")

print("######################################")
print("ASSIGNING UUID")
print("######################################")

# We need to create a spark UDF with the Python function in order to use it inside the Spark.
udfGenerateUUID = functions.udf(moduleExample.pythonFunctions.generate_uuid, StringType())

# Applying the python function
df_csv_sample = df_csv_sample.withColumn("uuid", udfGenerateUUID())

print("######################################")
print("PRINTING 10 ROWS OF SAMPLE DF")
print("######################################")

df_csv_sample.show(10)

