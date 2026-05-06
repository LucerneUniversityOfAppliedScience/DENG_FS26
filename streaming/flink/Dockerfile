FROM flink:1.20.0-java17

ARG FLINK_KAFKA_VERSION=3.3.0-1.20

RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_VERSION}.jar
