#!/usr/bin/env bash

TEMP_OUT_DIR="hw2_results_temp"
FINAL_OUT_DIR="hw2_results"
INPUT_DIR="/data/wiki/ru/articles"

hadoop fs -rm -r -skipTrash $TEMP_OUT_DIR*
hadoop fs -rm -r -skipTrash $FINAL_OUT_DIR*

mvn clean package

hadoop jar ~/big-data-hse/hw2/target/hw2-1.0-SNAPSHOT-jar-with-dependencies.jar $INPUT_DIR $TEMP_OUT_DIR $FINAL_OUT_DIR

hdfs dfs -cat hw2_results/part-r-00000 | head -10