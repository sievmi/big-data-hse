#!/usr/bin/env bash

hdfs dfs -rm -r hw5/task1

spark2-submit --class task1.SparkWordCount ./target/scala-2.11/hw5_2.11-0.1.jar

hdfs dfs -cat hw5/task1