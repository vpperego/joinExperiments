#!/usr/bin/env bash

nodes=(dbis-expsrv2 dbis-expsrv3 dbis-expsrv4 dbis-expsrv8 dbis-expsrv9 dbis-expsrv10 dbis-expsrv11 dbis-expsrv12)
mkdir resources/$1

for n in "${nodes[@]}"
do
mkdir -p resources/$1/${n}
scp -r vinicius@${n}:/tmp/$1* /home/vinicius/IdeaProjects/joinExperiments/resources/$1/${n}/
ssh vinicius@${n} "rm $1*"
done