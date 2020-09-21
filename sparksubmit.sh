#!/bin/bash
#
###############################################
#      Travel Analysis                        #
# configurations are in application.conf      #
#   -Dconfig.file=/${path}/application.conf   #
#                                             #
#                                             #
###############################################

 spark2-submit --master=yarn \
 --deploy-mode cluster \
 --driver-memory 1g \
 --num-executors 2 \
 --executor-cores 1 \
 --executor-memory 2g \
 --class com.quantexo.core.ProcessingApp \
 --files "application.conf,log4j.properties" \
 --driver-java-options="-Dconfig.file=application.conf" \
 --conf spark.executor.extraClassPath="-Dconfig.file=application.conf" \
 --conf spark.driver.extraClassPath="-Dconfig.file=application.conf" \
 --conf spark.driver.extraClassPath="-Dlog4j.configuration=log4j.properties" \
 --conf spark.executor.extraClassPath="-Dlog4j.configuration=log4j.properties" \
 target/TravelAnalysis-1.0-SNAPSHOT.jar

