#!/usr/bin/env bash

if [ "$1" = "start" ]; then
    ssh vinicius@dbis-expsrv1 "
        cd  kafka_2.11-2.0.0
        bin/zookeeper-server-start.sh config/zookeeper.properties &
        bin/kafka-server-start.sh config/server.properties &"
else
    ssh vinicius@dbis-expsrv1 "
        cd  kafka_2.11-2.0.0
        bin/zookeeper-server-stop.sh &
        bin/kafka-server-stop.sh"
fi