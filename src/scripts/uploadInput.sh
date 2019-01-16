#!/usr/bin/env bash

if [ $1 = "-f" ]
 then
    ssh vinicius@dbis-expsrv2 "rm -fr /home/vinicius/resources/*"
fi

scp -r /home/vinicius/IdeaProjects/joinExperiments/resources/* vinicius@dbis-expsrv2:/home/vinicius/resources

ssh vinicius@dbis-expsrv2 "hdfs dfs -rm -r /user/vinicius/resources/*;hdfs dfs -put /home/vinicius/resources/* /user/vinicius/resources"


scp -r vinicius@dbis-expsrv1:/home/vinicius/kafka.tar.gz .
scp -r vinicius@dbis-expsrv1:/home/vinicius/kafka.tar.gz vinicius@dbis-expsrv4:/home/vinicius/kafka.tar.gz

