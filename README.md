# Timely
---

Timely is a time series database application written in Java and designed to work with [Apache Accumulo] (http://accumulo.apache.org/) and [Grafana] (http://www.grafana.org).

# Design
---

When we required a time series database application, being users of Apache Accumulo, we looked to use compatible software. Specifically we started with [OpenTSDB] (http://www.opentsdb.net) and Eric Newton's [Accumulo-OpenTSDB] (https://github.com/ericnewton/accumulo-opentsdb) project to bridge them together, and Grafana for visualization. We ultimately ran into some issues that we could not work around. After much discussion we decided to replace OpenTSDB with something more native to Apache Accumulo. The implementation of Timely started with the following constraints and assumptions:

1. We need to show progress quickly
2. We started with Grafana, so we didn't need our own user interface
3. We already had experience with OpenTSDB, so we started with its API
4. Space is cheap, ingest and query speed are more important
5. Metrics will be kept short term
6. The number of tags used for each metric will be small \( < 10 \)
7. We can create a more native API and Grafana plugin at a later time

## Building

Timely uses Maven for compiling source, running tests, and packaging distributions. Below is a set of useful Maven lifecyles that we use:
 
Command | Description
--------|------------
mvn compile | Compiles and formats the source
mvn test | Compiles and formats source and runs unit tests
mvn package | Compiles and formats source, runs unit tests, and creates a distribution
mvn verify | Compiles and formats source, runs unit tests, creates a distribution, runs integration tests, and runs jacoco for code coverage
mvn verify site | Compiles and formats source, runs unit tests, creates a distribution, runs integration tests, and runs jacoco for code coverage, and creates the site
 
The [CollectD] (http://collectd.org/) plugins require that CollectD is installed locally as the /usr/share/collectd/java/collectd-api.jar file is a dependency.

## Deployment

The Timely server requires a Java 8 runtime. Timely utilizes iterators for Apache Accumulo, so your Accumulo instance will need to be run with Java 8 also.

Create a distribution and untar it somewhere. Modify the conf/timely.properties file appropriately. Then, copy the timely-server and commons-lang3 jar files to your Accumulo tservers. Next, launch Timely by running the bin/timely-server.sh script.

If you just want to kick the tires without having to install and setup Apache Hadoop and Apache Accumulo, then you can start Timely with the bin/timely-standalone.sh script. This will start an Accumulo MiniCluster in the background, but be aware that the standalone instance will not save your metric data across restarts.

## API

The current Timely API is compatible with a subset of the OpenTSDB API. We currently support the OpenTSDB TCP put API and we have partially implemented the following HTTP operations that the Grafana OpenTSDB datasource uses:

    * /api/aggregators
    * /api/query
    * /apu/search/lookup
    * /api/suggest

There is also an endpoint at /api/metrics that reports the metric names and tags that have been pushed to Timely. Eviction of this information is configurable, so it may reflect metrics that were reported but are no longer being reported. This could be useful in creating graphs in Grafana (so you won't have to go digging in the meta table in Accumulo). A native Timely API with security has been discussed and may be addressed in the future.

## Storage Format

Metrics sent to Timely are stored in two Accumulo tables, meta and metrics. The meta table stores information about the metric names and tags. It's format is:

Row | ColumnFamily | ColumnQualifier | Value
----|-------------:|----------------:|-----:
m:metric | | |
t:metric | tagKey| | 
v:metric | tagKey | tagValue |

The metrics table stores the datapoints using the following format:

Row | ColumnFamily | ColumnQualifier | Value
----|-------------:|----------------:|-----:
metric\timestamp | | tagKey=tagValue | metricValue

As an example, if you sent the following metric to the put api

```
put sys.cpu.user 1447879348291 2.0 rack=r001 host=r001n01 instance=0
```

it would get stored in the following manner in the meta table:

Row | ColumnFamily | ColumnQualifier | Value
----|-------------:|----------------:|-----:
m:sys.cpu.user | | |
t:sys.cpu.user | host | | 
t:sys.cpu.user | instance | | 
t:sys.cpu.user | rack | | 
v:sys.cpu.user | host | r001n01 | 
v:sys.cpu.user | instance | 0 | 
v:sys.cpu.user | rack | r001 | 

and in the following manner in the metrics table

Row | ColumnFamily | ColumnQualifier | Value
----|-------------:|----------------:|-----:
sys.cpu.user\1447879348291 | host=r001n01 | instance=0,rack=r001    | 2.0
sys.cpu.user\1447879348291 | instance=0   | host=r001n01,rack=r001  | 2.0
sys.cpu.user\1447879348291 | rack=r001    | host=r001n01,instance=0 | 2.0

## Configuration

The NUM\_SERVER\_THREADS variable in the timely-server.sh script controls how many threads are used in the Netty event group for TCP and HTTP operations. The TCP and HTTP groups use a different event group, so if you set the variable to 8, then you will have 8 threads for TCP operations and 8 threads for HTTP operations. The properties file in the conf directory expects the following properties:

Property | Description 
timely.ip | The ip address where the Timely server is running
timely.port.put | The port that will be used for processing put requests
timely.port.query | The port that will be used for processing query requests
timely.instance\_name | The name of your Accumulo instance
timely.zookeepers |  The list of Zookeepers for your Accumulo instance
timely.username | The username that Timely will use to connect to Accumulo
timely.password | The password of the Accumulo user
timely.table | The name of the metrics table
timely.meta | The name of the meta table
timely.write.latency | The Accumulo BatchWriter latency
timely.write.threads | The Accumulo BatchWriter thread size
timely.write.buffer.size | The Accumulo BatchWriter buffer size
timely.metric.age.off.days | The number of days to keep metrics
timely.cors.allow.any.origin | Allow any origin in cross origin requests (true/false)
timely.cors.allow.null.origin | Allow null origins in cross origin requests (true/false)
timely.cors.allowed.origins | List of allowed origins in cross origin requests (can be null or comma separated list)
timely.cors.allowed.methods | List of allowed methods for cross origin requests (DELETE,GET,HEAD,OPTIONS,PUT,POST)
timely.cors.allowed.headers | Comma separated list of allowed HTTP headers for cross origin requests
timely.cors.allow.credentials | Allow credentials to be passed in cross origin requests (true/false)
timely.scanner.threads | Number of BatchScanner threads to be used in a query
timely.metrics.report.tags.ignored | Comma separated list of tags which will not be shown in the /api/metrics response
timely.meta.cache.expiration.minutes | Number of minutes after which unaccessed meta information will be purged from the meta cache
timely.meta.cache.initial.capacity | Initial capacity of the meta cache
timely.meta.cache.max.capacity | Maximum capacity of the meta cache

## Tuning

1. Each thread in the Timely server that is used for processing TCP put operations has its own BatchWriter. Each BatchWriter honors the ```timely.write.latency``` and ```timely.write.threads``` configuration property, but the buffer size for each BatchWriter is ```timely.write.buffer.size``` divided by the number of threads. For example, if you have 8 threads processing put operations and the following settings:

```
timely.write.latency=30s
timely.write.threads=2
timely.write.buffer.size=1G
``` 

then you will have 8 BatchWriters each using 2 threads with a 30s latency and a maximum buffer size of 128M.

2. The ```timely.scanner.threads``` property is used for BatchScanners on a per query basis. If you set this to 32 and have 8 threads processing HTTP operations, then you might have 256 threads concurrently querying your tablet servers. Be sure to set your ulimits appropriately.

3. The Timely server contains an object called the meta cache, which is a cache of the keys that would be inserted into the meta table if they do not already exist. This cache is used to reduce the insert load of duplicate keys into the meta table and to serve up data to the /api/metrics endpoint. The meta cache is an object that supports eviction based on last access time and can be tuned with the ```timely.meta.cache.*```  properties.


## NOTES

1. Not shown in the examples is the encoding of the row and value. You can apply the Timely formatter to your metrics table using the Accumulo shell command:
```
config -t timely.metrics -s table.formatter=timely.util.TimelyMetricsFormatter
```

2. You can create split points for your metrics table based on the 'm' column family in the meta table.

3. You can lower the ```table.scan.max.memory``` property on your metrics table in an attempt to get data back faster from the tablet servers.

4. If you don't mind losing some metric data in the event of an Accumulo tablet server death, you can set the ```table.walog.enabled``` property to false on your metrics table. This should speed up ingest a little.
