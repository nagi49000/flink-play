ARG FLINK_VERSION=1.14.4
ARG SCALA_VERSION=2.12
FROM flink:${FLINK_VERSION}-scala_${SCALA_VERSION}-java8
ARG FLINK_VERSION=1.14.4
ARG SCALA_VERSION=2.12

# install python3: it has updated Python to 3.9 in Debian 11 and so install Python 3.7 from source
# it currently only supports Python 3.6, 3.7 and 3.8 in PyFlink officially.

RUN apt-get update -y && \
    apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev && \
    wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz && \
    tar -xvf Python-3.7.9.tgz && \
    cd Python-3.7.9 && \
    ./configure --without-tests --enable-shared && \
    make -j6 && \
    make install && \
    ldconfig /usr/local/lib && \
    cd .. && rm -f Python-3.7.9.tgz && rm -rf Python-3.7.9 && \
    ln -s /usr/local/bin/python3 /usr/local/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install apache-flink==${FLINK_VERSION} && \
    mkdir /opt/flink/pyflink-jobs

# add jars for Kafka connectors (kafka version 3.1.0)
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.1.0/kafka-clients-3.1.0.jar \
    /opt/flink/opt/
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/${FLINK_VERSION}/flink-connector-base-${FLINK_VERSION}.jar \
    /opt/flink/opt/
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka_${SCALA_VERSION}/${FLINK_VERSION}/flink-connector-kafka_${SCALA_VERSION}-${FLINK_VERSION}.jar \
    /opt/flink/opt/

RUN chmod 755 /opt/flink/opt/*jar
