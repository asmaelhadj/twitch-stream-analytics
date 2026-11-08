#!/bin/bash

echo "Starting Spark Application Launcher..."
echo "Script to run: ${SPARK_SCRIPT}"

# Wait for dependencies
echo "Waiting for Kafka to be ready..."
while ! nc -z kafka 9092; do
    echo "Waiting for Kafka..."
    sleep 5
done
echo "Kafka is ready!"

echo "Waiting for Elasticsearch to be ready..."
while ! nc -z elasticsearch 9200; do
    echo "Waiting for Elasticsearch..."
    sleep 5
done
echo "Elasticsearch is ready!"

# Additional wait to ensure services are fully initialized
echo "Waiting 30 seconds for services to fully initialize..."
sleep 30

# Set Spark configuration
export SPARK_HOME=/opt/bitnami/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH

# Determine which script to run
if [ -z "$SPARK_SCRIPT" ]; then
    echo "Error: SPARK_SCRIPT environment variable not set"
    exit 1
fi

echo "Running Spark script: $SPARK_SCRIPT"

# Run the specified Spark script
exec $SPARK_HOME/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.elasticsearch:elasticsearch-spark-30_2.12:7.17.14,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
    /app/$SPARK_SCRIPT