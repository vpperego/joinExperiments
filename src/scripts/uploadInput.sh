#!/usr/bin/env bash

scp -r /home/vinicius/IdeaProjects/joinExperiments/resources/* vinicius@dbis-expsrv2:/home/vinicius/resources

ssh vinicius@dbis-expsrv2 "hdfs dfs -rm -r /user/vinicius/resources/*;hdfs dfs -put /home/vinicius/resources/* /user/vinicius/resources"
