#!/usr/bin/env bash

hdfs dfs -rm -r hw5/task5

spark2-submit --class task5.PopularBrowserCache ./target/scala-2.11/hw5_2.11-0.1.jar

hdfs dfs -cat hw5/task5/*