FROM rocker/rstudio:4.0.0

ARG R_VERSION=4.0.0

#RUN /rocker_scripts/install_python.sh

#RUN /rocker_scripts/install_tidyverse.sh

RUN /rocker_scripts/install_shiny_server.sh

# what specific versions?
#RUN apt-get -y update && apt-get install -y \
#   default-jdk \
#   r-cran-rjava \
#   && apt-get clean \
#   && rm -rf /var/lib/apt/lists/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir pandas matplotlib

RUN install2.r --error --skipinstalled --ncpus -1 \
    shinydashboard \
    here
#   rJava \
#    here \
#    sparklyr

#RUN /opt/R/${R_VERSION}/bin/R CMD javareconf JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

