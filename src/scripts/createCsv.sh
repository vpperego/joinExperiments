#!/usr/bin/env bash

if [ "$#" = 0 ]; then
    echo "Usage: $0 dir/ output-file.csv"
    exit
fi
FILTERS="[.numInputRows,.processedRowsPerSecond,.durationMs.addBatch,.durationMs.getBatch,.durationMs.getOffset,.durationMs.queryPlanning,.durationMs.triggerExecution,.durationMs.walCommit,.sources[0].numInputRows,.sources[0].processedRowsPerSecond,.sources[1].numInputRows,.sources[1].processedRowsPerSecond]|@csv"


echo "numInputRows,processedRowsPerSecond,addBatch,getBatch,getOffset,queryPlanning,triggerExecution,walCommit,numInputRowsA,processedRowsPerSecond,numInputRowsB,processedRowsPerSecond" > $2
for DIR in $1; do
	echo "$DIR"
	for SUB_DIR in $DIR/*; do
  	ls $SUB_DIR
	cat  $SUB_DIR/* | jq $FILTERS |  sed -e 's/^"//' -e 's/"$//' >> $2
	done
done
