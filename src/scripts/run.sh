#!/bin/bash

sbt package
mkdir /tmp/spark-events
#TODO - need to test
# zookeeper-server-start.sh $KAFKA/config/zookeeper.properties
#sleep 5
# kafka-server-start.sh $KAFKA/config/server.properties

#Delete the topics
 # kafka-topics.sh --zookeeper localhost:2181 --delete --topic ratings
 # kafka-topics.sh --zookeeper localhost:2181 --delete --topic actors
 # kafka-topics.sh --zookeeper localhost:2181 --delete --topic titles
 # kafka-topics.sh --zookeeper localhost:2181 --delete --topic actors_titles
 # kafka-topics.sh --zookeeper localhost:2181 --delete --topic imdb_output
# bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic actors
# bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic titles
# bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic actors_titles

#kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic actors

rm -fr resources/checkpoints/
rm -fr resources/stream_output
#rm -fr output2


#spark-submit --master local[*] --class=producerStreaming --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 target/scala-2.11/streamingscala_2.11-0.1.jar
#/home/vinicius/IdeaProjects/spark/bin/spark-submit --driver-memory 1g --master local[*] --class $1 target/scala-2.11/streamingscala_2.11-0.1.jar
#spark-submit  --master local[*] --class="$1" target/scala-2.11/streamingscala_2.11-0.1.jar
#spark-submit --driver-memory 2g --master local[*] --class $1 target/scala-2.11/streamingscala_2.11-0.1.jar
#/home/vinicius/IdeaProjects/spark/bin/spark-submit --master yarn --deploy-mode cluster --class="$1" target/scala-2.11/streamingscala_2.11-0.1.jar
#sleep 5Q
# spark-submit --master local[*] --class=imdbJoin --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 target/scala-2.11/streamingscala_2.11-0.1.jar
# spark-submit --master local[*] --class=newJoin --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 target/scala-2.11/streamingscala_2.11-0.1.jar


#/home/vinicius/Desktop/spark/bin/spark-submit --master local[*] --driver-memory 2g  --class="$1" target/scala-2.11/streamingscala_2.11-0.1.jar

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master local[*] --class "$1" target/scala-2.11/joinexperiments_2.11-0.1.jar
#/home/vinicius/Desktop/spark/bin/spark-submit --master yarn --deploy-mode cluster --driver-memory 2g --executor-memory 2g  --executor-cores 2 --class="$1" target/scala-2.11/streamingscala_2.11-0.1.jar