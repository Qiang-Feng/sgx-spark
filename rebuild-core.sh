build/mvn -T 1C -DskipTests package -pl :spark-core_2.11 &&
rm assembly/target/scala-2.11/jars/spark-core_2.11-2.4.0-SGX.jar && cp core/target/spark-core_2.11-2.4.0-SGX.jar assembly/target/scala-2.11/jars/
