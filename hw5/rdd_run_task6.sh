#!/usr/bin/env bash

hdfs dfs -rm -r hw5/rdd/task6

spark2-submit --class rdd.task6.PopularBrowserBroadcast ./target/scala-2.11/hw5_2.11-0.1.jar

hdfs dfs -cat hw5/rdd/task6/*