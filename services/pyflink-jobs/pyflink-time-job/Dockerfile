# this image should be available from the pyflink build stage
FROM services_jobmanager:latest

WORKDIR app

COPY flink_time_transform.py ./

CMD ["python", "flink_time_transform.py"]