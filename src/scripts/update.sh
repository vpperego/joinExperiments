#!/usr/bin/env bash
ZOOKEEPER=localhost:2181
kafka-topics.sh --zookeeper $ZOOKEEPER --delete --topic relA
kafka-topics.sh --zookeeper $ZOOKEEPER --delete --topic relB
kafka-topics.sh --zookeeper $ZOOKEEPER --delete --topic relC
kafka-topics.sh --zookeeper $ZOOKEEPER --delete --topic storedJoin
kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions 1 --topic relA
kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions 1 --topic relB
kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions 1 --topic relC
kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions 1 --topic storedJoin
