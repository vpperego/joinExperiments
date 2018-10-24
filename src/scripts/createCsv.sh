#!/usr/bin/env bash

if [ "$#" = 0 ]; then
    echo "Usage: $0 dir/ output-file.csv"
    exit
fi
FILTERS="[.numInputRows,.processedRowsPerSecond,.durationMs.addBatch,.durationMs.getBatch,.durationMs.getOffset,.durationMs.queryPlanning,.durationMs.triggerExecution,.durationMs.walCommit,.sources[0].numInputRows,.sources[0].processedRowsPerSecond,.sources[1].numInputRows,.sources[1].processedRowsPerSecond]|@csv"


echo "numInputRows,processedRowsPerSecond, addBatch,getBatch,getOffset,queryPlanning,triggerExecution,walCommit,numInputRowsA,processedRowsPerSecond,numInputRowsB,processedRowsPerSecond\n" > $2
for DIR in $1/; do
	for SUB_DIR in $DIR/*/; do
  	cat $SUB_DIR/0.json | jq $FILTERS >> $2
	done
done
