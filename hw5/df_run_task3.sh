#!/usr/bin/env bash

hdfs dfs -rm -r hw5/dataframes/task3

spark2-submit --class dataframe.task3.Regions ./target/scala-2.11/hw5_2.11-0.1.jar

hdfs dfs -cat hw5/dataframes/task3