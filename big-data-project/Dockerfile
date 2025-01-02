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
        python3-pip \
        ssh \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setup the directories for our Spark and Hadoop installations
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download and install Spark
RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Download the Kafka connector JAR
RUN curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.3/spark-sql-kafka-0-10_2.12-3.3.3.jar -o $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.3.3.jar

# Download Kafka Clients JAR and related dependencies
RUN curl -L https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar -o $SPARK_HOME/jars/kafka-clients-2.8.1.jar \
    && curl -L https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar -o $SPARK_HOME/jars/slf4j-api-1.7.36.jar \
    && curl -L https://repo1.maven.org/maven2/org/slf4j/slf4j-log4j12/1.7.36/slf4j-log4j12.jar -o $SPARK_HOME/jars/slf4j-log4j12-1.7.36.jar

RUN curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.3/spark-token-provider-kafka-0-10_2.12-3.3.3.jar -o $SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.3.3.jar

# Download commons-pool2
RUN curl -o $SPARK_HOME/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

FROM spark-base as pyspark

# Install Python dependencies including pandas
RUN pip3 install --no-cache-dir \
    pandas>=1.5.0 \
    pymongo>=4.0.0 \
    python-dotenv>=0.19.0 \
    findspark>=1.4.0 \
    numpy>=1.21.0 \
    hdfs>=2.7.0  \
    pyarrow


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
ENTRYPOINT ["./entrypoint.sh"]

