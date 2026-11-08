FROM bitnami/spark:3.4

USER root

RUN pip install elasticsearch pyspark

# Copy both jobs
COPY spark_twitch_to_es.py /app/spark_twitch_to_es.py
COPY spark_kafka_to_hdfs.py /app/spark_kafka_to_hdfs.py

# Default to ES job, override via CMD
WORKDIR /app
ENV SPARK_SCRIPT=spark_twitch_to_es.py


COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
CMD ["/entrypoint.sh"]
