# Windows-Compatible Spark Dockerfile
FROM bitnami/spark:3.4

USER root

# Update package list and install required packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    curl \
    netcat-traditional \
    dos2unix \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download Elasticsearch Hadoop connector
RUN wget -O /opt/bitnami/spark/jars/elasticsearch-spark-30_2.12-7.17.14.jar \
    https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/7.17.14/elasticsearch-spark-30_2.12-7.17.14.jar

# Create app directory
RUN mkdir -p /app

# Copy Python scripts
COPY spark_streams_processor.py /app/
COPY spark_chat_processor.py /app/
COPY spark_batch_processor.py /app/
COPY spark_kafka_to_hdfs.py /app/

# Copy entrypoint and fix Windows line endings
COPY entrypoint.sh /app/entrypoint.sh
RUN dos2unix /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Create checkpoint directories
RUN mkdir -p /tmp/spark_checkpoints /tmp/chat_checkpoints

# Set working directory
WORKDIR /app

# Use the entrypoint
ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]