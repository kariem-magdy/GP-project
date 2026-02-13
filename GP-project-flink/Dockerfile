FROM flink:1.19.0-java11

# Copy the fat JAR into Flink's user library directory
# The standalone-job entrypoint automatically adds /opt/flink/usrlib/ to the classpath
COPY target/GP-project-flink-1.0-SNAPSHOT.jar /opt/flink/usrlib/GP-project-flink-1.0-SNAPSHOT.jar
