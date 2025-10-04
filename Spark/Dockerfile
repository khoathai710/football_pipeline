FROM python:3.11-bullseye

# Cài Java 17 và dọn cache trong 1 RUN
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME="/home/sparkuser/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Tạo thư mục Spark và download Spark
RUN mkdir -p ${SPARK_HOME} && \
    curl -s https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz -o spark-3.5.6-bin-hadoop3.tgz && \
    tar xvzf spark-3.5.6-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 && \
    rm -rf spark-3.5.6-bin-hadoop3.tgz

ENV SPARK_MASTER_PORT="7077"
ENV SPARK_MASTER_HOST="spark-master"

# Download aws jars cùng lúc vào spark/jars
RUN wget -q -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Tạo user sparkuser và set quyền
RUN useradd -u 1000 -m -d /home/sparkuser sparkuser && \
    chown -R 1000:1000 ${SPARK_HOME}

ENV HOME="/home/sparkuser"
USER sparkuser

# Copy config Spark
COPY ./spark-defaults.conf "${SPARK_HOME}/conf"

ENTRYPOINT ["/bin/bash"]
