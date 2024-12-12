FROM python:3.10-bullseye as spark-base

ARG SPARK_VERSION=3.3.3
ARG KAFKA_CLIENTS_VERSION=3.3.2

# Install tools required by the OS
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setup the directories for our Spark and Hadoop installations
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# # Xóa các file JAR Kafka cũ (thêm phần này)
# RUN rm -f $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-*.jar
# RUN rm -f $SPARK_HOME/jars/kafka-clients-*.jar

# Download and install Spark
RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
&& tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
&& rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Download the Kafka connector JAR
RUN curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.3/spark-sql-kafka-0-10_2.12-3.3.3.jar -o $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.3.3.jar

# Download Kafka Clients JAR (Thêm phần này)
RUN curl -L https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar -o $SPARK_HOME/jars/kafka-clients-2.8.1.jar \
    && curl -L https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar -o $SPARK_HOME/jars/slf4j-api-1.7.36.jar \
    && curl -L https://repo1.maven.org/maven2/org/slf4j/slf4j-log4j12/1.7.36/slf4j-log4j12.jar -o $SPARK_HOME/jars/slf4j-log4j12-1.7.36.jar

RUN curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.3/spark-token-provider-kafka-0-10_2.12-3.3.3.jar -o $SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.3.3.jar

# Tải commons-pool2 từ Maven Repository
RUN curl -o $SPARK_HOME/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Thiết lập quyền và chuyển về người dùng Spark
# RUN chown -R spark:spark /opt/spark/jars
# USER spark

FROM spark-base as pyspark

# Install python deps
COPY spark/requirements.txt .
RUN pip3 install -r requirements.txt

# Setup Spark related environment variables
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# Copy the default configurations into $SPARK_HOME/conf
COPY spark/conf/spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copy appropriate entrypoint script
COPY entrypoint.sh .
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]
# CMD ["/bin/sh", "-c", "entrypoint.sh"]


 