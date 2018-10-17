#!/bin/bash

sbt package
mkdir /tmp/spark-events


rm -fr resources/checkpoints/
rm -fr resources/stream_output

if [ "$1" = "cluster" ]; then
    RES_LOCATION = "hdfs:/user/vinicius/resources/config/""$3"
    ssh vinicius@dbis-expsrv2 "hdfs dfs -rm -r /user/vinicius/checkpoint"
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master yarn --deploy-mode cluster target/scala-2.11/joinexperiments_2.11-0.1.jar "$2" $RES_LOCATION
else
    RES_LOCATION = "file:///home/vinicius/IdeaProjects/joinExperiments/resources/config/""$2"
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master local[*] target/scala-2.11/joinexperiments_2.11-0.1.jar "$1" $RES_LOCATION
fi