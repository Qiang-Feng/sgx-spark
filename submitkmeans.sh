#!/bin/bash

source variables.sh

export IS_ENCLAVE=false
export IS_DRIVER=true
export IS_WORKER=false

export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver

export SPARK_JOBNAME=kmeans

rm -rf $(pwd)/output

./bin/spark-submit \
--class org.apache.spark.examples.mllib.KMeansExample \
--master spark://kiwi01.doc.res.ic.ac.uk:7077 \
--deploy-mode client \
--verbose \
--executor-memory 1g \
--name $SPARK_JOBNAME \
--conf "spark.app.id=$SPARK_JOBNAME" \
--conf "spark.executor.extraLibraryPath=$(pwd)/lib" \
--conf "spark.driver.extraLibraryPath=$(pwd)/lib" \
--conf "spark.driver.extraClassPath=$(pwd)/assembly/target/scala-${SCALA_VERSION}/jars/*:$(pwd)/examples/target/scala-${SCALA_VERSION}/jars/*" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/conf/log4j.properties" \
examples/target/scala-${SCALA_VERSION}/jars/spark-examples_${SCALA_VERSION}-${SPARK_VERSION}-SNAPSHOT.jar $(pwd)/data/mllib/kmeans_data.txt 2>&1 output | tee outside-driver
