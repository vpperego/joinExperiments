#!/usr/bin/env bash

kafka-topics.sh --zookeeper dbis-expsrv2:2181 --delete --topic relA
kafka-topics.sh --zookeeper dbis-expsrv2:2181 --delete --topic relB
kafka-topics.sh --zookeeper dbis-expsrv2:2181 --delete --topic relC
kafka-topics.sh --zookeeper dbis-expsrv2:2181 --delete --topic storedJoin
kafka-topics.sh --create --zookeeper dbis-expsrv2:2181 --replication-factor 1 --partitions 6 --topic relA
kafka-topics.sh --create --zookeeper dbis-expsrv2:2181 --replication-factor 1 --partitions 6 --topic relB
kafka-topics.sh --create --zookeeper dbis-expsrv2:2181 --replication-factor 1 --partitions 6 --topic relC
kafka-topics.sh --create --zookeeper dbis-expsrv2:2181 --replication-factor 1 --partitions 12 --topic storedJoin
