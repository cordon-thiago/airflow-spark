# RSTUDIO

FROM rocker/rstudio:4.0.0

ARG SPARK_VERSION
ARG HADOOP_VERSION

ENV SPARK_HOME /usr/local/spark

RUN /rocker_scripts/install_shiny_server.sh
RUN /rocker_scripts/install_python.sh

RUN /rocker_scripts/install_tidyverse.sh

RUN apt-get -y update && apt-get install -y \
   openjdk-8-jdk \
   && apt-get clean \
   && rm -rf /var/lib/apt/lists/

# Install further R packages
RUN install2.r --repo https://mran.microsoft.com/snapshot/2021-08-09  \
    shiny  \
    shinydashboard \
    here \
    zoo \
    dygraphs \
    shinyAce \
    DT \
    uuid \
   && rm -rf /tmp/downloaded_packages/ /tmp/*.rds

# python dependencies (possibility of unifying them?)
COPY docker/python_requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install -r requirements.txt
