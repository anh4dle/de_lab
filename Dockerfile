FROM apache/airflow:2.10.2

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow 

COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt

RUN pip uninstall -y pyspark
RUN pip install pyspark==3.5.0