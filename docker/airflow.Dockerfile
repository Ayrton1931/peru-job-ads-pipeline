FROM apache/airflow:2.9.3-python3.11

# Java for Spark (local mode inside the Airflow container)
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY docker/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==2.9.3" -r /tmp/requirements-airflow.txt
