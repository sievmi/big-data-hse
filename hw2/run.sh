#!/usr/bin/env bash

OUT_DIR="hw2_results"
INPUT_DIR="/data/wiki/ru/articles"

hadoop fs -rm -r -skipTrash $OUT_DIR*

mvn clean package

hadoop jar ~/big-data-hse/hw2/target/hw2-1.0-SNAPSHOT-jar-with-dependencies.jar $INPUT_DIR $OUT_DIR