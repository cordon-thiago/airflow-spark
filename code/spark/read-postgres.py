import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create spark session
#conf = SparkConf().setAppName("Spark Read Postgres").setMaster("spark://spark:7077").setConfig()
#spark = SparkSession.builder.config(conf=conf).getOrCreate()
#sc = spark.sparkContext

spark = SparkSession \
    .builder \
    .appName("Spark Read Postgres") \
    .master("spark://spark:7077") \
    .config("spark.jars", "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar") \
    .getOrCreate()
    
sc = spark.sparkContext

####################################
# Parameters
####################################
#postgres_db = sys.argv[1]
#postgres_user = sys.argv[2]
#postgres_pwd = sys.argv[3]

postgres_db = "test"
postgres_user = "test"
postgres_pwd = "postgres"

####################################
# Read Postgres
####################################
print("######################################")
print("READING POSTGRES TABLES")
print("######################################")

df_pets = (
    spark.read
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "pets")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .load()
)

####################################
# Tpo 10 movies with more ratings
####################################
df_movies = df_movies.alias("m")
df_ratings = df_ratings.alias("r")

df_join = df_ratings.join(df_movies, df_ratings.movieId == df_movies.movieId).select("r.*","m.title")

df_result = (
    df_join
    .groupBy("title")
    .agg(
        F.count("timestamp").alias("qty_ratings")
        ,F.mean("rating").alias("avg_rating")
    )
    .sort(F.desc("qty_ratings"))
    .limit(10)
)

print("######################################")
print("EXECUTING QUERY AND SAVING RESULTS")
print("######################################")
# Save result to a CSV file
df_result.coalesce(1).write.format("csv").mode("overwrite").save("/usr/local/spark/resources/data/output_postgres", header=True)

