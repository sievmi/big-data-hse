#!/usr/bin/env bash

hdfs dfs -rm -r hw5/task7

spark2-submit --class task7.PopularBrowsersCountry ./target/scala-2.11/hw5_2.11-0.1.jar

hdfs dfs -cat hw5/task7/*