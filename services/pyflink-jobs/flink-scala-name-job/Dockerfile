ARG FLINK_VERSION=1.14.3
FROM flink:${FLINK_VERSION}-scala_2.11-java8

RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
    chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg && \
    apt-get update && \
    apt-get -y install sbt

WORKDIR /job
COPY ./namejob/ ./

# build jars via sbt
RUN sbt clean assembly && \
    find ./ -name "*jar"

# using CLI and options described on https://nightlies.apache.org/flink/flink-docs-release-1.2/setup/cli.html
CMD ["flink", "run", "-m", "jobmanager-flink-play:8081", "-c", "org.flinkplay.FlinkNameJob", "/job/target/scala-2.11/NameJob-assembly-0.1-SNAPSHOT.jar"]