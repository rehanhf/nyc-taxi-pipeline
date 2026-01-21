FROM apache/spark:3.5.0

USER root

#hadoop AWS and AWS SDK JARs for S3
RUN curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

#verify 
RUN ls -lh /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    ls -lh /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER 185