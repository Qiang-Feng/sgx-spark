FROM openjdk:8-jre-alpine

ARG SPARK_HOME=/home/qiang/Documents/sgx-spark
ENV SPARK_HOME=$SPARK_HOME

COPY assembly/target/scala-2.11/jars/spark-launcher_2.11-2.4.0-SGX.jar /sgx-spark/assembly/target/scala-2.11/jars/spark-launcher_2.11-2.4.0-SGX.jar
COPY assembly/target/scala-2.11/jars/spark-core_2.11-2.4.0-SGX.jar /sgx-spark/assembly/target/scala-2.11/jars/spark-core_2.11-2.4.0-SGX.jar
COPY assembly/target/scala-2.11/jars/scala-library-2.11.12.jar /sgx-spark/assembly/target/scala-2.11/jars/scala-library-2.11.12.jar
COPY assembly/target/scala-2.11/jars/slf4j-api-1.7.16.jar /sgx-spark/assembly/target/scala-2.11/jars/slf4j-api-1.7.16.jar
COPY assembly/target/scala-2.11/jars/slf4j-log4j12-1.7.16.jar /sgx-spark/assembly/target/scala-2.11/jars/slf4j-log4j12-1.7.16.jar
COPY assembly/target/scala-2.11/jars/log4j-1.2.17.jar /sgx-spark/assembly/target/scala-2.11/jars/log4j-1.2.17.jar
COPY assembly/target/scala-2.11/jars/hadoop-common-2.7.3.jar /sgx-spark/assembly/target/scala-2.11/jars/hadoop-common-2.7.3.jar
COPY assembly/target/scala-2.11/jars/commons-lang-2.6.jar /sgx-spark/assembly/target/scala-2.11/jars/commons-lang-2.6.jar
COPY assembly/target/scala-2.11/jars/aircompressor-0.10.jar /sgx-spark/assembly/target/scala-2.11/jars/aircompressor-0.10.jar

RUN mkdir -p $SPARK_HOME
RUN mv /sgx-spark/* $SPARK_HOME/