FROM dinkydocker/dinky-standalone-server:1.2.5-flink1.20

# Kafka SQL connector for Dinky's bundled Flink (used when Dinky runs jobs
# in "Local" mode rather than against the external session cluster).
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
        -o /opt/dinky/extends/flink1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

# MySQL JDBC driver — DinkyMysqlCatalog requires it on the classpath even
# when running in H2 standalone mode, otherwise DefaultCatalog persistence
# fails on startup.
RUN curl -fSL https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar \
        -o /opt/dinky/lib/mysql-connector-j-8.3.0.jar
