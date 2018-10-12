#!/bin/bash

sbt package
mkdir /tmp/spark-events


rm -fr resources/checkpoints/
rm -fr resources/stream_output

if [ "$1" = "cluster" ]; then
    ssh vinicius@dbis-expsrv2 "hdfs dfs -rm -r /user/vinicius/checkpoint"
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master yarn --deploy-mode cluster --class "$2" target/scala-2.11/joinexperiments_2.11-0.1.jar
else
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master local[*] --class "$1" target/scala-2.11/joinexperiments_2.11-0.1.jar
fi