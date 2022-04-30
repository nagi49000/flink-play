# this image should be available from the pyflink build stage
# test build
FROM services_jobmanager:latest

WORKDIR /test

COPY *.py ./

RUN pip3 install pytest && \
    python3 -m pytest

# actual image build for job
FROM services_jobmanager:latest
COPY flink_time_transform.py /opt/flink/pyflink-jobs/
# using CLI for running pyflink jobs described at https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/cli/#submitting-pyflink-jobs
CMD ["flink", "run", "-m", "jobmanager-flink-play:8081", "-py", "/opt/flink/pyflink-jobs/flink_time_transform.py"]
