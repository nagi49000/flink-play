# this image should be available from the pyflink build stage
# test build
FROM services_jobmanager:latest

# uber-hack here
# manually add test files from github repo, since they aren't packaged with the apache-flink distro
ARG FLINK_VERSION=1.14.4
# avro version pulled from pom https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.14.4/flink-avro-1.14.4.pom
# doing this manually courtesy of https://issues.apache.org/jira/browse/FLINK-17417
ARG AVRO_VERSION=1.10.0
ARG PYTHON_ROOT=/usr/local/lib/python3.7/
ARG PYFLINK_TESTING_DIR=${PYTHON_ROOT}/site-packages/pyflink/testing/
ARG PYFLINK_TESTING_URL=https://raw.githubusercontent.com/apache/flink/master/flink-python/pyflink/testing

RUN pip3 install pytest-cov kafka-python && \
    mkdir -m 755 /usr/local/lib/python3.7/site-packages/pyflink/testing/ && \
    mkdir -m 755 -p /opt/flink/flink-formats/flink-avro/target/

ADD ${PYFLINK_TESTING_URL}/__init__.py \
    ${PYFLINK_TESTING_DIR}
ADD ${PYFLINK_TESTING_URL}/source_sink_utils.py \
    ${PYFLINK_TESTING_DIR}
ADD ${PYFLINK_TESTING_URL}/test_case_utils.py \
    ${PYFLINK_TESTING_DIR}

# add jars for unit testing
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-avro/${FLINK_VERSION}/flink-avro-${FLINK_VERSION}.jar \
    /opt/flink/opt/
ADD https://repo1.maven.org/maven2/org/apache/avro/avro/${AVRO_VERSION}/avro-${AVRO_VERSION}.jar \
    ${PYTHON_ROOT}/flink-formats/flink-avro/target/

# run tests
WORKDIR /test

COPY *.py ./

RUN python3 -m pytest -s

# actual image build for job
FROM services_jobmanager:latest
COPY flink_time_transform.py /opt/flink/pyflink-jobs/
# using CLI for running pyflink jobs described at https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/cli/#submitting-pyflink-jobs
CMD ["flink", "run", "-m", "jobmanager-flink-play:8081", "-py", "/opt/flink/pyflink-jobs/flink_time_transform.py"]
