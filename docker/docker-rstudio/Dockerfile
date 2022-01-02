# RSTUDIO

FROM rocker/rstudio:4.1.0

ARG SPARK_VERSION
ARG HADOOP_VERSION

ENV SPARK_HOME /usr/local/spark

RUN /rocker_scripts/install_shiny_server.sh
RUN /rocker_scripts/install_python.sh

RUN /rocker_scripts/install_tidyverse.sh

# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
        wget -q --no-verbose "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        mkdir -p "${SPARK_HOME}/bin" && \
        mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
        rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

RUN apt-get -y update && apt-get install -y \
   openjdk-8-jdk \
   && apt-get clean \
   && rm -rf /var/lib/apt/lists/

# Install further R packages
RUN install2.r --error --skipinstalled --deps TRUE \
   shinydashboard \
   here \
   sparklyr \
   rJava \
   RJDBC \
   odbc \
   zoo \
   dygraphs \
   && rm -rf /tmp/downloaded_packages/ /tmp/*.rds

RUN R CMD javareconf

# python dependencies (possibility of unifying them?)
COPY docker/docker-rstudio/requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install -r requirements.txt
