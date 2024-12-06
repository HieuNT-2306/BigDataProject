FROM bitnami/spark:3.3
USER root
RUN apt-get update && apt-get install -y python3-pip
COPY app/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
WORKDIR /app
