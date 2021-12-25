library(sparklyr)
library(DBI)
library(dplyr)
# It works! https://spark.rstudio.com/


sc <- spark_connect(master = "spark://spark:7077")

# copy mtcars into spark
mtcars_tbl <- copy_to(sc, mtcars)
dbGetQuery(sc, "SELECT * FROM mtcars LIMIT 10")

partitions <- mtcars_tbl %>%
  filter(hp >= 100) %>%
  mutate(cyl8 = cyl == 8) %>%
  sdf_random_split(training = 0.5, test = 0.5, seed = 1099)

fit <- partitions$training %>%
  ml_linear_regression(response = "mpg", features = c("wt", "cyl"))
fit
