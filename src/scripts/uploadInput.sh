#!/usr/bin/env bash

scp -r /home/vinicius/IdeaProjects/joinExperiments/resources/relA/ vinicius@dbis-expsrv2:/home/vinicius/relA
scp -r /home/vinicius/IdeaProjects/joinExperiments/resources/relB/ vinicius@dbis-expsrv2:/home/vinicius/relB

ssh vinicius@dbis-expsrv2 "hdfs dfs -rm /user/vinicius/relA/*;hdfs dfs -put /home/vinicius/relA/* /user/vinicius/;
hdfs dfs -rm /user/vinicius/relB/*;hdfs dfs -put /home/vinicius/relB/* /user/vinicius/"
