#!/usr/bin/env bash

hdfs dfs -rm -r hw5/rdd/task3

spark2-submit --class rdd.task3.StopWordsCount ./target/scala-2.11/hw5_2.11-0.1.jar

hdfs dfs -cat hw5/rdd/task3