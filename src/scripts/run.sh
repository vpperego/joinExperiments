#!/bin/bash

if [ "$#" = 0 ]; then
    echo "Usage $0 [cluster] app-name config-file"
    exit 1
fi
rm -fr resources/output/

sbt package
mkdir /tmp/spark-events
mkdir /tmp/spark-log


rm -fr resources/checkpoints/
rm -fr resources/stream_output

#SPARK=/home/vinicius/spark-2.4.0-bin-hadoop2.6/bin/spark-submit
SPARK=spark-submit
PACKAGES=org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0
TARGET=target/scala-2.11/joinexperiments_2.11-0.1.jar


if [ "$1" = "cluster" ]; then
    RES_LOCATION="hdfs:/user/vinicius/resources/config/""$3"
    ssh vinicius@dbis-expsrv2 "hdfs dfs -rm -r /user/vinicius/checkpoint"
    $SPARK --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.0  --master yarn \
    --files=file:///usr/local/spark/conf/metrics.properties --conf spark.metrics.conf=metrics.properties \
 	--num-executors 24 \
	--executor-cores 5 \
	--driver-memory 12g \
	--executor-memory 8g \
	--deploy-mode cluster $TARGET "$2" $RES_LOCATION
#    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master yarn --deploy-mode cluster target/scala-2.11/joinexperiments_2.11-0.1.jar "$2" $RES_LOCATION
else
    RES_LOCATION="file:///Users/vinicius/IdeaProjects/joinGermany/joinExperiments/resources/config/""$2"
    $SPARK --packages $PACKAGES --master local[*] \
    --driver-memory 4g \
    $TARGET "$1" $RES_LOCATION
fi

