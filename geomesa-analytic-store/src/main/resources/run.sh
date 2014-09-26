#!/bin/bash

# maybe this will work?

echo "Starting Activity Indicator..."

JAR="target/geomesa-analytic-store-1.0.0-SNAPSHOT.jar"

# args
IN_TOPIC="ts_input"
QUOTE="true"
ALERT_TOPIC="ts_alerts"
TIMESERIES_TOPIC="ts_aggregated"
WINDOW="4"
INPUT_DATE_FMT="yyyy-MM-dd'T'HH:mm:ss.SSSZ"
MODE="local"
TOPOLOGY_NAME="activityIndicator"
ALERT_SFT_NAME="alerts"
TIMESERIES_SFT_NAME="timeseries"
ZOO="localhost:2181"
ACC_USER="myuser"
ACC_PASS="mypassword"
ACC_INST="dcloud"
GEOMESA_CATALOG_TABLE="actind_catalog"
BROKER="localhost:9092"
MOCK="true"
SITES_FILE="src/main/resources/sites.mapping"

# cluster (replace line below)
#storm jar target/geomesa-analytic-store-1.0.0-SNAPSHOT.jar org.locationtech.geomesa.analytic.storm.StormExecutor \

# local
java -cp target/geomesa-analytic-store-1.0.0-SNAPSHOT.jar org.locationtech.geomesa.analytic.storm.StormExecutor \
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
--broker $BROKER \
--mock-geomesa $MOCK \
--site-geom-file $SITES_FILE


