import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

####################################
# Parameters
####################################
postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

####################################
# Read Postgres
####################################
print("######################################")
print("READING POSTGRES TABLES")
print("######################################")

df_movies = (
    spark.read
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.movies")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .load()
)

df_ratings = (
    spark.read
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.ratings")
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

