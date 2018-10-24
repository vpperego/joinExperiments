#!/bin/bash

if [ "$#" = 0 ]; then
    echo "Usage $0 [cluster] app-name config-file"
    exit 1
fi

sbt package
mkdir /tmp/spark-events
rm -fr resources/checkpoints/
rm -fr resources/stream_output

SPARK=/home/vinicius/IdeaProjects/spark-2.3.2/bin/spark-submit

for i in {1..5}
do
    if [ "$1" = "cluster" ]; then
        (cat resources/config/kafka-inner-join.conf; echo "kafkaTopicOutput crossJoin5K$i") > resources/config/kafka-inner-join2.conf
        src/scripts/uploadInput.sh
        RES_LOCATION="hdfs:/user/vinicius/resources/config/""$3"
        ssh vinicius@dbis-expsrv2 "hdfs dfs -rm -r /user/vinicius/checkpoint"
        $SPARK --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master yarn --deploy-mode cluster target/scala-2.11/joinexperiments_2.11-0.1.jar "$2"-$i $RES_LOCATION
#        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master yarn --deploy-mode cluster target/scala-2.11/joinexperiments_2.11-0.1.jar "$2" $RES_LOCATION

    else
        echo "local nummer ist $i"
        RES_LOCATION="file:///home/vinicius/IdeaProjects/joinExperiments/resources/config/""$2"
        (cat resources/config/kafka-inner-join.conf; echo "kafkaTopicOutput localNewJoin$i") > resources/config/kafka-inner-join2.conf

        $SPARK --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master local[*] target/scala-2.11/joinexperiments_2.11-0.1.jar "$1"-$i $RES_LOCATION
    #    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master local[*] target/scala-2.11/joinexperiments_2.11-0.1.jar "$1" $RES_LOCATION
    fi
done