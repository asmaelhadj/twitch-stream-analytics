#!/bin/bash

echo "Running Spark job: $SPARK_SCRIPT"

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.2 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy \
  /app/$SPARK_SCRIPT
