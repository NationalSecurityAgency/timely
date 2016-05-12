# Timely
---

Timely is a time series database application that provides secure access to time series data. Timely is written in Java and designed to work with [Apache Accumulo] (http://accumulo.apache.org/) and [Grafana] (http://www.grafana.org).

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

## Security

Timely allows users to optionally label their data using Accumulo column visibility expressions. To enable this feature, users should put the expressions in a tag named ```viz```. Timely will take this expression as-is and store it in the column visibility in the metrics table for the data point. Note that column visibilities are not stored in the meta table, so anyone can see metric names, tag names, and tag values.

Timely provides HTTPS access to the query endpoints. Anonymous access to these endpoints can be enabled which will allow anyone to see unlabeled data. Timely uses Spring Security to configure user authentication and user role information. Users will need to call the login endpoint for authentication and Timely will respond by setting a HTTP cookie with a session id. The response to a successful login will be a temporary redirect (HTTP 307) to the configured address for the Grafana server.

## Building

Timely uses Maven for compiling source, running tests, and packaging distributions. Below is a set of useful Maven lifecyles that we use:
 
Command | Description
--------|------------
mvn compile | Compiles and formats the source
mvn test | Runs unit tests
mvn package | Runs findbugs and creates a distribution
mvn verify | Runs integration tests
mvn verify site | Creates the site
 
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

We also provide the following additional endpoints:

    * /login

This endpoint should be used for non-anonymous access. It supports a POST request with username and password, or a GET request with SSL mutual authentication.

    * /api/metrics

This endpoint reports the metric names and tags that have been pushed to Timely. Eviction of this information is configurable, so it may reflect metrics that were reported but are no longer being reported. This could be useful in creating graphs in Grafana (so you won't have to go digging in the meta table in Accumulo).

## Storage Format

Metrics sent to Timely are stored in two Accumulo tables, meta and metrics. The meta table stores information about the metric names and tags. It's format is:

Row | ColumnFamily | ColumnQualifier | Value
----|:-------------|:----------------|:-----
m:metric | | |
t:metric | tagKey | |
v:metric | tagKey | tagValue |

The metrics table stores the datapoints using the following format:

Row | ColumnFamily | ColumnQualifier | Value
----|:-------------|:----------------|:-----
metric\timestamp | tagKey=tagValue | tagKey=tagValue,tagKey=tagValue,... | metricValue

As an example, if you sent the following metric to the put api

```
put sys.cpu.user 1447879348291 2.0 rack=r001 host=r001n01 instance=0
```

it would get stored in the following manner in the meta table:

Row | ColumnFamily | ColumnQualifier | Value
----|:-------------|:----------------|:-----
m:sys.cpu.user | | |
t:sys.cpu.user | host | | 
t:sys.cpu.user | instance | | 
t:sys.cpu.user | rack | | 
v:sys.cpu.user | host | r001n01 | 
v:sys.cpu.user | instance | 0 | 
v:sys.cpu.user | rack | r001 | 

and in the following manner in the metrics table

Row | ColumnFamily | ColumnQualifier | Value
----|:-------------|:----------------|:-----
sys.cpu.user\1447879348291 | host=r001n01 | instance=0,rack=r001    | 2.0
sys.cpu.user\1447879348291 | instance=0   | host=r001n01,rack=r001  | 2.0
sys.cpu.user\1447879348291 | rack=r001    | host=r001n01,instance=0 | 2.0

## Configuration

The NUM\_SERVER\_THREADS variable in the timely-server.sh script controls how many threads are used in the Netty event group for TCP and HTTP operations. The TCP and HTTP groups use a different event group, so if you set the variable to 8, then you will have 8 threads for TCP operations and 8 threads for HTTP operations. The properties file in the conf directory expects the following properties:

Property | Description | Default Value
:--------|:------------|:-------------
timely.ip | The ip address where the Timely server is running |
timely.port.put | The port that will be used for processing put requests |
timely.port.query | The port that will be used for processing query requests |
timely.instance\_name | The name of your Accumulo instance |
timely.zookeepers |  The list of Zookeepers for your Accumulo instance |
timely.username | The username that Timely will use to connect to Accumulo |
timely.password | The password of the Accumulo user |
timely.table | The name of the metrics table | timely.metrics
timely.meta | The name of the meta table | timely.meta
timely.write.latency | The Accumulo BatchWriter latency | 5s
timely.write.threads | The Accumulo BatchWriter number of threads | BatchWriterConfig.DEFAULT_MAX_WRITE_THREADS
timely.write.buffer.size | The Accumulo BatchWriter buffer size | BatchWriterConfig.DEFAULT_MAX_MEMORY
timely.metric.age.off.days | The number of days to keep metrics | 7
timely.cors.allow.any.origin | Allow any origin in cross origin requests (true/false) | false
timely.cors.allow.null.origin | Allow null origins in cross origin requests (true/false) | false
timely.cors.allowed.origins | List of allowed origins in cross origin requests (can be null or comma separated list)
timely.cors.allowed.methods | List of allowed methods for cross origin requests | DELETE,GET,HEAD,OPTIONS,PUT,POST
timely.cors.allowed.headers | Comma separated list of allowed HTTP headers for cross origin requests | content-type
timely.cors.allow.credentials | Allow credentials to be passed in cross origin requests (true/false) | true
timely.scanner.threads | Number of BatchScanner threads to be used in a query | 4
timely.metrics.report.tags.ignored | Comma separated list of tags which will not be shown in the /api/metrics response |
timely.meta.cache.expiration.minutes | Number of minutes after which unaccessed meta information will be purged from the meta cache | 60
timely.meta.cache.initial.capacity | Initial capacity of the meta cache | 2000
timely.meta.cache.max.capacity | Maximum capacity of the meta cache | 10000
timely.ssl.certificate.file | Public certificate to use for the Timely server |
timely.ssl.key.file | Private key to use for the Timely server |
timely.ssl.key.pass | Password to the private key |
timely.ssl.use.generated.keypair | Use a generated certificate/key pair - useful for testing | false
timely.ssl.require.client.authentication | Enable 2-way SSL | true
timely.ssl.trust.store.file | Certificate trust store |
timely.ssl.use.openssl | Use OpenSSL (vs JDK SSL) | true
timely.ssl.use.ciphers | List of allowed SSL ciphers | see Configuration.java
timely.session.max.age | Setting for max age of session cookie (in seconds) | 86400
timely.http.address | Address for the Timely server, used for the session cookie domain |
grafana.http.address | Address for the Grafana server |
timely.allow.anonymous.access | Allow anonymous access | false
timely.visibility.cache.expiration.minutes | Column Visibility Cache Expiration (minutes) | 60
timely.visibility.cache.initial.capacity | Column Visibility Cache Initial Capacity | 2000
timely.visibility.cache.max.capacity | Column Visibility Cache Max Capacity | 10000

## Tuning

1. Each thread in the Timely server that is used for processing TCP put operations has its own BatchWriter. Each BatchWriter honors the ```timely.write.latency``` and ```timely.write.threads``` configuration property, but the buffer size for each BatchWriter is ```timely.write.buffer.size``` divided by the number of threads. For example, if you have 8 threads processing put operations and the following settings:

```
timely.write.latency=30s
timely.write.threads=2
timely.write.buffer.size=1G
``` 

then you will have 8 BatchWriters each using 2 threads with a 30s latency and a maximum buffer size of 128M.

2. The ```timely.scanner.threads``` property is used for BatchScanners on a per query basis. If you set this to 32 and have 8 threads processing HTTP operations, then you might have 256 threads concurrently querying your tablet servers. Be sure to set your ulimits appropriately.

3. The Timely server contains an object called the meta cache, which is a cache of the keys that would be inserted into the meta table if they did not already exist. This cache is used to reduce the insert load of duplicate keys into the meta table and to serve up data to the /api/metrics endpoint. The meta cache is an object that supports eviction based on last access time and can be tuned with the ```timely.meta.cache.*```  properties.


## NOTES

1. Not shown in the examples is the encoding of the row and value. You can apply the Timely formatter to your metrics table using the Accumulo shell command:
```
config -t timely.metrics -s table.formatter=timely.util.TimelyMetricsFormatter
```

2. You can create split points for your metrics table based on the 'm' column family in the meta table.

3. You can lower the ```table.scan.max.memory``` property on your metrics table in an attempt to get data back faster from the tablet servers.

4. If you don't mind losing some metric data in the event of an Accumulo tablet server death, you can set the ```table.walog.enabled``` property to false and the ```table.durability``` property to none on your metrics table. This should speed up ingest a little.
