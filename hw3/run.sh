#!/usr/bin/env bash

OUT_DIR="hw3_results"
INPUT_DIR="/user/pakhtyamov/PUBG_data"

hadoop fs -rm -r -skipTrash $OUT_DIR*

mvn clean package

hadoop jar ~/big-data-hse/hw3/target/hw3-1.0-SNAPSHOT-jar-with-dependencies.jar $INPUT_DIR $OUT_DIR

hdfs dfs -cat $OUT_DIR/final/part-r-00000 | head