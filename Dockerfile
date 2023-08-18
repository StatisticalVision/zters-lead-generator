# This file contains instructions to create final docker image for zters-data-backup conatiner

# Base image --------------------------------------------------------------
FROM rocker/r-ver:4.1.1

# copy scripts and or data -----------------------------------------------------
# We next write instructions to create directories as needed in the container and copy the scripts
# Create a directory called /zters-data-backup and install the libpq5 package required for connecting to PostgreSQL databases.
RUN mkdir -p /zters-lead-generator &&\
    apt-get update &&\
    apt-get install libpq5 -y

WORKDIR /zters-lead-generator
COPY R R/
COPY config config/
COPY install_packages.r /zters-lead-generator/install_packages.r
COPY Main - Lead Generator - Postgres DB.r /zters-lead-generator/Main - Lead Generator - Postgres DB.r

# Run the script ---------------------------------------------------------------
RUN Rscript /zters-lead-generator/install_packages.r
CMD Rscript /zters-lead-generator/Main - Lead Generator - Postgres DB.r
