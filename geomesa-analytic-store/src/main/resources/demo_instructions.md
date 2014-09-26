# Introduction

Instructions for running a simple time series demo using Kafka, Storm, and GeoMesa to stream data through a simple time
series analysis bolt that will alert based on daily count activity for named data series.

A named data series is simply a data series with a name and unique ID. The unique ID provides a common threading key on
which to build a time series and issue alerts. This allows data to be interleaved in the kafka queues and end up in
different time series for anomaly detection. For example, you can stream data series A and B in any order (e.g. a, a, b,
a, b, b, b, a, a, a) and all the a's will end up in TimeSeries A and b's in TimeSeries B.

In order to facilitate geospatial analytics, a named TimeSeries can have an associated Geometry which will be associated
with the TimeSeries or Anomaly alert when stored in GeoMesa. This enables follow-on analysis in a geospatial context.
This type of analysis may be called a Site Activity Indicator (aka monitor activity at a site). One such example would
be remote motion sensor activity which has some known noise threshold. Instead of alerting on every sign of activity, it
is more insightful (and less annoying) to issue alerts when the activity is anomalous.

# Requried Software

The following software is needed:

* Kafka 0.8.1.1
* Storm 0.9.2-incubating
* GeoMesa 1.0.0-SNAPSHOT (a running cluster)

# Installation Instructions

Installation can be done in a standalone mode or on a real kafka/zookeeper/storm cluster. Both modes should work and
insert data into GeoMesa. For demo purposes the standalone methodology is useful.

## Local Installation Instructions
Derived from online apache kafka documentation...

### 1. Install Kafka

    tar -xzf kafka_2.9.2-0.8.1.1.tgz
    cd kafka_2.9.2-0.8.1.1

### 2. Start a single node zookeeper

    bin/zookeeper-server-start.sh config/zookeeper.properties &

### 3. Start the kafka server

    bin/kafka-server-start.sh config/server.properties &

### 4. Create Input and output topics.
The input topic will be used to feed data into the TimeSeries topology. The output topic is for use by any other
processes which wish to receive alerts or aggregated time series information.
   
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ts_input
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ts_alerts
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ts_aggregated

### 5. Start a console producer in a new tab

This producer will be used to insert data into kafka in "time-series" CSV format (discussed below)

    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ts_input

### 6. Start alert and aggregated timeseries console consumers

    bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ts_alerts
    
...and in another tab

    bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ts_aggregated
    
### 7. Start up the LocalCluster (demo) Storm Topology:

    java -cp target/geomesa-analytic-store-1.0.0-SNAPSHOT.jar org.locationtech.geomesa.analytic.storm.StormExecutor -i ts_input -a ts_alerts

### 8. Send some Events to the Console Producer:

TimeSeries CSV format consists of three fields: date (in ISO8601 format), site_name, count

For example...here are two sites (over 7 days) with interleaved, multiday, multicount inputs:

    "2014-01-01T05:05:05.000+0000","Virginia","1"
    "2014-01-01T05:05:05.000+0000","Virginia","3"
    "2014-01-01T05:05:05.000+0000","North Carolina","3"
    "2014-01-02T05:05:05.000+0000","Virginia","2"
    "2014-01-02T05:05:05.000+0000","Virginia","1"
    "2014-01-02T05:05:05.000+0000","Virginia","1"
    "2014-01-02T05:05:05.000+0000","North Carolina","1"
    "2014-01-02T05:05:05.000+0000","North Carolina","2"
    "2014-01-03T05:05:05.000+0000","Virginia","2"
    "2014-01-03T05:05:05.000+0000","North Carolina","2"
    "2014-01-04T05:05:05.000+0000","North Carolina","1"
    "2014-01-04T05:05:05.000+0000","Virginia","3"
    "2014-01-04T05:05:05.000+0000","North Carolina","2"
    "2014-01-05T05:05:05.000+0000","Virginia","5"
    "2014-01-05T05:05:05.000+0000","North Carolina","5"
    "2014-01-06T05:05:05.000+0000","North Carolina","21"
    "2014-01-06T05:05:05.000+0000","Virginia","5"
    "2014-01-06T05:05:05.000+0000","Virginia","17"
    "2014-01-07T05:05:05.000+0000","North Carolina","2"
    "2014-01-07T05:05:05.000+0000","Virginia","3"

Simply copy/paste this into the Kafka Console Producer created in Step 5 to send it into the Storm topology via Kafka.

### Reboot: Deleting Zookeeper & Kafka Data and starting all over

In local mode, you can delete everything and start over by deleting data from ```/tmp```

    rm /tmp/zookeeper/ -rf
    rm /tmp/kafka-logs/ -rf



