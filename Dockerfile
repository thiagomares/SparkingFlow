FROM apache/airflow:latest-python3.12

USER root

# Install required packages
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

# Install Airflow and Spark packages
RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark

# Optional: Add any additional setup or configurations here
