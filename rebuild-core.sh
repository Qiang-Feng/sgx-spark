build/mvn -T1C -DskipTests -pl core install &&
rm assembly/target/scala-2.11/jars/spark-core_2.11-2.4.0-SGX.jar && cp core/target/spark-core_2.11-2.4.0-SGX.jar assembly/target/scala-2.11/jars/
