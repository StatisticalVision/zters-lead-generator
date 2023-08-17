# This file contains instructions to create final docker image for zters-data-backup conatiner

# Base image --------------------------------------------------------------
FROM rocker/r-ver:4.1.1

# copy scripts and or data -----------------------------------------------------
# We next write instructions to create directories as needed in the container and copy the scripts
# Create a directory called /zters-data-backup and install the libpq5 package required for connecting to PostgreSQL databases.
RUN mkdir -p /zters-data-backup &&\
    apt-get update &&\
    apt-get install libpq5 -y

WORKDIR /zters-data-backup
COPY R R/
COPY config config/
COPY install_packages.r /zters-data-backup/install_packages.r
COPY main_incremental_refresh.r /zters-data-backup/main_incremental_refresh.r

# Run the script ---------------------------------------------------------------
RUN Rscript /zters-data-backup/install_packages.r
CMD Rscript /zters-data-backup/main_incremental_refresh.r