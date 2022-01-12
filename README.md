# Airflow Spark / Dask

This project deploys: 

- Timescale database (based on Postgres) for Airflow metadata plus database `test`.

- Airflow. Webserver and scheduler in the same container. Webserver available at [http://localhost:8282](http://localhost:8282/)

- Dask cluster, consisting on an scheduler plus a variable number of workers. The scheduler is available at [http://localhost:8787/status](http://localhost:8787/status)

- Rstudio instance (with Shiny Server and Python + Reticulate). This serves both as workbench for development(Rstudio, available at [http://localhost:8787](http://localhost:8787/)) and a way to host Shiny apps (app available at [http://localhost:3838/app/](http://localhost:3838/app/).

## On some decisions (why is not Spark here?)

Running so many resource-demanding Docker containers in a single machine proves to be taxing. With all the containers up, decent performance is achieved dedicating 4 CPUs and 10 GB of memory.

Hence, be sure to configure your Docker resources accordingly. 

Spark proves to be particularly slow in this setup. Currently I am experimenting with Dask as an alternative to demonstrate distributed computing in local; lighter and python native.

## Quickstart

You will need docker engine in your local.

In one command, `docker compose up` will build and start all the containers defined in `docker-compose.yml` (run the command from the root of the project).

Downloading images and building from Dockerfiles will take a while (perhaps 20 minutes)

Some Airflow DAGs expect the folder `./local_data` to be present, so you will have to create it in your local. Of course, in a production environment, remote storage like a S3 bucket would be used instead.

## Passwords

Right now the setup is grossly unsafe; this is just for testing purposes

- Timescale/Postgres
  - Server: `localhost:5432`
  - Database: `test`
  - User: `test`
  - Password: `postgres`

- Airflow: Totally unprotected (this is set up in the configuration file)

- Dask: Unprotected

- Rstudio
  - User: `rstudio`
  - Password: `airflow`

## Using Spark instead of Dask

Dependencies of Spark are probably all around the project at the moment, even when in `docker-compose.yml`, only Dask is specified. 

To use spark scheduler and workers, something like the code beneath shall be used.
```
spark:
   image: *spark_image
   user: root # Run container as root container: https://docs.bitnami.com/tutorials/work-with-non-root-containers/
   hostname: spark
   networks:
       - default_net
   environment:
       - SPARK_MODE=master
       - SPARK_RPC_AUTHENTICATION_ENABLED=no
       - SPARK_RPC_ENCRYPTION_ENABLED=no
       - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
       - SPARK_SSL_ENABLED=no
   volumes:
       - *spark_path
       - *resources_path
       - *data_storage_path
   ports:
       - "8181:8080"
       - "7077:7077"

spark-worker:
   image: *spark_image
   user: root
   networks:
       - default_net
   environment: *spark-worker-env
   volumes:
       - *spark_path
       - *resources_path
       - *data_storage_path
```
