# Airflow Spark

This project contains the following containers:

* postgres: Postgres database for Airflow
    * Image: postgres:9.6
    * Database Port: 5432
    * References: https://hub.docker.com/_/postgres

* airflow-webserver: Airflow webserver and Scheduler
    * Image: puckel/docker-airflow:latest
    * Port: 8080
    * References: https://github.com/puckel/docker-airflow

* spark: Spark Master
    * Image: bitnami/spark:latest
    * Port: 8181
    * References: https://github.com/bitnami/bitnami-docker-spark

* spark-worker-N: Spark workers. You can add workers copying the containers and changing the container name
    * Image: bitnami/spark:latest
    * References: https://github.com/bitnami/bitnami-docker-spark

* jupyter-spark: Jupyter notebook with pyspark for interactive development
  * Image: jupyter/pyspark-notebook
  * Port: 8888
  * References: 
    * https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook
    * https://hub.docker.com/r/jupyter/pyspark-notebook/tags/

## Setup

### Download Images

    sudo docker pull postgres:9.6
    sudo docker pull puckel/docker-airflow:latest
    sudo docker pull bitnami/spark:latest
    sudo docker pull jupyter/pyspark-notebook:latest

### Start containers

    sudo docker-compose up

### Check if you can access

Airflow: http://localhost:8080

Spark Master: http://localhost:8181

Jupyter Notebook: http://127.0.0.1:8888
  * For Jupyter notebook, you must copy the URL with the token generated when the container is started and paste in your browser. The URL with the token can be taken from container logs using:
  
        docker logs -f airflow-spark_jupyter-spark_1

## Adding Airflow Extra packages

Rebuild Dockerfile:

    sudo docker build --rm --build-arg AIRFLOW_DEPS="gcp" -t puckel/docker-airflow .

After successfully built, run docker-compose to start container:

    sudo docker-compose up

More info at: https://github.com/puckel/docker-airflow#build

## Useful docker commands

    List Images:
    sudo docker images <repository_name>

    List Containers:
    sudo docker container ls

    Check container logs:
    sudo docker logs -f <container_name>

    To build a Dockerfile after changing sth (run inside directoty containing Dockerfile):
    sudo docker build --rm -t <tag_name> .

    Access container bash:
    sudo docker exec -i -t <container_name> /bin/bash

## Useful docker-compose commands

    Start Containers:
    sudo docker-compose -f <compose-file.yml> up -d

    Stop Containers:
    sudo docker-compose -f <compose-file.yml> down --remove-orphans
    

