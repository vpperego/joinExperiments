#!/usr/bin/env bash

if [ "$1" = "start" ]; then
    ssh vinicius@dbis-expsrv1 "
        cd  kafka_2.11-2.0.0
        bin/zookeeper-server-start.sh config/zookeeper.properties &
        bin/kafka-server-start.sh config/server.properties &
        ;"
#    ssh vinicius@dbis-expsrv2 "
#    cd  kafka_2.11-2.0.0
#    bin/kafka-server-start.sh config/server.properties &
#    ;"
#    ssh vinicius@dbis-expsrv4 "
#    cd  kafka_2.11-2.0.0
#    bin/kafka-server-start.sh config/server.properties &
#    ;"


#else if [ "$1" = "createTopic" ]; then
#    ssh vinicius@dbis-expsrv1 "
#    cd  kafka_2.11-2.0.0
#    bin/kafka-kafkaTopic.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions $3 --topic $2
#    "
else
  ssh vinicius@dbis-expsrv1 "
    cd  kafka_2.11-2.0.0
    bin/kafka-kafkaTopic.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions $2 --topic $1
    ssh vinicius@dbis-expsrv1 "
#        cd  kafka_2.11-2.0.0
#        bin/zookeeper-server-stop.sh &
#        bin/kafka-server-stop.sh"
fi