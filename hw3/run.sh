#!/usr/bin/env bash

TEMP_OUT_DIR="hw3_results_temp"
#FINAL_OUT_DIR="hw3_results"
INPUT_DIR="/user/pakhtyamov/PUBG_data"

hadoop fs -rm -r -skipTrash $TEMP_OUT_DIR*
#hadoop fs -rm -r -skipTrash $FINAL_OUT_DIR*

mvn clean package

hadoop jar ~/big-data-hse/hw3/target/hw3-1.0-SNAPSHOT-jar-with-dependencies.jar $INPUT_DIR $TEMP_OUT_DIR

# hdfs dfs -cat hw3_results/part-r-00000 | head -10