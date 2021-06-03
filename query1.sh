#!/bin/bash
$SPARK_HOME/bin/spark-submit --class "query.Query1" --master "local" target/SABD-Project_1-1.0-SNAPSHOT.jar $1