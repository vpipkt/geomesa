#!/bin/bash

# maybe this will work?

echo "Starting Activity Indicator..."

JAR="geomesa-analytic-store-1.0.0-SNAPSHOT.jar"

# args
IN_TOPIC="ts_in"
QUOTE="false"
ALERT_TOPIC="alerts"
TIMESERIES_TOPIC="timeseries"
WINDOW="20"
INPUT_DATE_FMT="yyyy-MM-dd'T'HH:mm:ss.SSSZ"
MODE="local"
TOPOLOGY_NAME="activityIndicator"
ALERT_SFT_NAME="alerts"
TIMESERIES_SFT_NAME="timeseries"
ZOO="localhost:2181"
ACC_USER="root"
ACC_PASS="secret"
ACC_INST="dcloud"
GEOMESA_CATALOG_TABLE="actind_catalog"
BROKER="localhost:9092"

# local
java -cp target/geomesa-analytic-store-1.0.0-SNAPSHOT.jar org.locationtech.geomesa.analytic.storm.StormExecutor \
# cluster
#storm jar target/geomesa-analytic-store-1.0.0-SNAPSHOT.jar org.locationtech.geomesa.analytic.storm.StormExecutor \
-i $IN_TOPIC \
-q $QUOTE \
-a $ALERT_TOPIC \
-t $TIMESERIES_TOPIC \
-w $WINDOW \
-d $INPUT_DATE_FMT \
-m $MODE \
-n $TOPOLOGY_NAME \
--alertSftName $ALERT_SFT_NAME \
--timeSeriesSftName $TIMESERIES_SFT_NAME \
-z $ZOO \
-u $ACC_USER \
-p $ACC_PASS \
--instance $ACC_INST \
--catalog $GEOMESA_CATALOG_TABLE \
--broker $BROKER


