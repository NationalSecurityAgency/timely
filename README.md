# Timely

Timely is a time series database application that provides secure access to time series data. Timely is written in Java and designed to work with [Apache Accumulo] (http://accumulo.apache.org/) and [Grafana] (http://www.grafana.org).

## Design
---

Being big users of Apache Accumulo, when we required a time series database application we looked to use software that was compatible with it. Specifically, we started with [OpenTSDB] (http://www.opentsdb.net) and Eric Newton's [Accumulo-OpenTSDB] (https://github.com/ericnewton/accumulo-opentsdb) project to bridge them together and Grafana for visualization. We ultimately ran into some issues that we could not work around. After much discussion we decided to replace OpenTSDB with something more native to Apache Accumulo. The implementation of Timely started with the following constraints and assumptions:

1. We need to show progress quickly.
2. We started with Grafana, so we didn't need our own user interface.
3. We already had experience with OpenTSDB, so we started with its API.
4. Space is cheap, ingest and query speed are more important.
5. Metrics will be kept short term.
6. The number of tags used for each metric will be small \( < 10 \).
7. We can create a better API and Grafana plugin at a later time.

## Building
---

Timely uses Maven for compiling source, running tests, and packaging distributions. Below is a set of useful Maven lifecyles that we use:
 
Command | Description
--------|------------
mvn compile | Compiles and formats the source
mvn test | Runs unit tests
mvn package | Runs findbugs and creates a distribution
mvn verify | Runs integration tests
mvn verify site | Creates the site
 
  :warning: The [CollectD] (http://collectd.org/) plugins require that CollectD is installed locally as the `/usr/share/collectd/java/collectd-api.jar` file is a dependency.

  :warning: If you are having trouble with the server pom in your IDE, take a look [here] (https://github.com/trustin/os-maven-plugin#issues-with-eclipse-m2e-or-other-ides)

  :warning: Timely uses the [netty-tcnative] (http://netty.io/wiki/forked-tomcat-native.html) module to provide native access to OpenSSL. The correct version of the artifact is downloaded during the build. If you are building for a different platform, then you can override the classifier by specifying the `os.detected.classifier` property on the Maven command line. See the netty-tcnative wiki for more information.

## Deployment
---

The Timely server requires a Java 8 runtime. Timely utilizes iterators for Apache Accumulo, so your Accumulo instance will need to be run with Java 8 also.

Create a distribution and untar it somewhere. Modify the `conf/timely.properties` file appropriately. Then, copy the `timely-server` jar file to your Accumulo tservers. Next, launch Timely by running the `bin/timely-server.sh` script.

If you just want to kick the tires without having to install and setup Apache Hadoop and Apache Accumulo, then you can start Timely with the `bin/timely-standalone.sh` script. This will start an Accumulo MiniCluster in the background, but be aware that the standalone instance will not save your metric data across restarts.

## Application Interfaces
---

### Sending data to Timely

Timely accepts data using the OpenTSDB [TCP] (http://opentsdb.net/docs/build/html/user_guide/writing.html#telnet) method. This you the flexibility to use the command line, OpenTSDB's [TCollector] (https://github.com/OpenTSDB/tcollector), or roll your own solution.

### Getting data ouf of Timely

#### HTTP API

If anonymous access is disabled on the Timely server, then you will first need to log in by making a GET or POST call to the `/login` endpoint. HTTP GET requests are used in conjunction with client certificate authentication and POST requests are used with Basic Authentication. See the [Security] (#security) section below.

The current Timely HTTP API is compatible with a subset of the OpenTSDB API that the Grafana OpenTSDB datasource uses:

    * /api/aggregators
    * /api/query
    * /api/search/lookup
    * /api/suggest

Additionally, the following endpoint reports the metric names and tags that have been pushed to Timely. Eviction of this information is configurable, so it may reflect metrics that have been reported in the past but are no longer being actively reported. This could be useful in creating graphs in Grafana (so you won't have to go digging in the `meta` table in Accumulo).

    * /api/metrics

#### Web Sockets

Timely supports creating subscriptions to metric data via WebSockets at the `/websocket` endpoint. If anonymous access is disabled, then a call to the `/login` endpoint should be made first. The requests made to the API require a sessionId as part of the request. When anonymous access is disabled, the sessionId should be the value of the TSESSIONID cookie returned from the login request. When anonymous access is enabled, the sessionId can be a unique string (e.g. UUID). The WebSocket API supports the following requests:

##### create

The create request does some up front work for creating metric subscriptions on this connection. Example request:

```
{
  "operation" : "create",
  "sessionId" : "1234-555-27282"
}
```

##### add

The add request adds a subscription for metric data matching the supplied parameters. The add request can be called multiple times to register multiple subscriptions for different metrics on the same connection. For each add request a thread is started that will send matching data to the caller via the WebSocket. Example request:

```
{
  "operation" : "add",
  "sessionId" : "1234-555-27282",
  "metric" : "sys.cpu.user",
  "tags" : {
             "tag1" : "value1",
             "tag2" : "value2"
           },
  "startTime" : "1467306000"
}
```

  :question: Note that tags are optional. The startTime parameter is also optional, and if not specified will default to zero which will return all matching metrics in the Accumulo Timely metrics table.

##### remove

The remove request stops the thread that is returning data and removes the subscription. Example request:

```
{
  "operation" : "remove",
  "sessionId" : "1234-555-27282",
  "metric" : "sys.cpu.user"
}
```

##### close

The close request removes all subscriptions on this connection and closes the connection. Example request:

```
{
  "operation" : "close",
  "sessionId" : "1234-555-27282"
}
```

## Storage Format
---

Metrics sent to Timely are stored in two Accumulo tables, `meta` and `metrics`. The `meta` table stores information about the metric names and tags. It's format is:

Row | ColumnFamily | ColumnQualifier | Value
----|:-------------|:----------------|:-----
m:metric | | |
t:metric | tagKey | |
v:metric | tagKey | tagValue |

The `metrics` table stores the datapoints using the following format:

Row | ColumnFamily | ColumnQualifier | Value
----|:-------------|:----------------|:-----
metric\timestamp | tagKey=tagValue | tagKey=tagValue,tagKey=tagValue,... | metricValue

As an example, if you sent the following metric to the put api

```
put sys.cpu.user 1447879348291 2.0 rack=r001 host=r001n01 instance=0
```

it would get stored in the following manner in the `meta` table:

Row | ColumnFamily | ColumnQualifier | Value
----|:-------------|:----------------|:-----
m:sys.cpu.user | | |
t:sys.cpu.user | host | | 
t:sys.cpu.user | instance | | 
t:sys.cpu.user | rack | | 
v:sys.cpu.user | host | r001n01 | 
v:sys.cpu.user | instance | 0 | 
v:sys.cpu.user | rack | r001 | 

and in the following manner in the `metrics` table

Row | ColumnFamily | ColumnQualifier | Value
----|:-------------|:----------------|:-----
sys.cpu.user\1447879348291 | host=r001n01 | instance=0,rack=r001    | 2.0
sys.cpu.user\1447879348291 | instance=0   | host=r001n01,rack=r001  | 2.0
sys.cpu.user\1447879348291 | rack=r001    | host=r001n01,instance=0 | 2.0

## Configuration
---

The `NUM_SERVER_THREADS` variable in the `timely-server.sh` script controls how many threads are used in the Netty event group for TCP and HTTP operations. The TCP and HTTP groups use a different event group, so if you set the value to 8, then you will have 8 threads for TCP operations and 8 threads for HTTP operations. The properties file in the `conf` directory supports the following properties:

Property | Description | Default Value
:--------|:------------|:-------------
timely.ip | The ip address where the Timely server is running |
timely.port.put | The port that will be used for processing put requests |
timely.port.query | The port that will be used for processing query requests |
timely.port.websocket | The port that will be used for processing web socket requests |
timely.instance\_name | The name of your Accumulo instance |
timely.zookeepers |  The list of Zookeepers for your Accumulo instance |
timely.username | The username that Timely will use to connect to Accumulo |
timely.password | The password of the Accumulo user |
timely.table | The name of the metrics table | timely.metrics
timely.meta | The name of the meta table | timely.meta
timely.write.latency | The Accumulo BatchWriter latency | 5s
timely.write.threads | The Accumulo BatchWriter number of threads | [default](https://github.com/apache/accumulo/blob/master/core/src/main/java/org/apache/accumulo/core/client/BatchWriterConfig.java#L49)
timely.write.buffer.size | The Accumulo BatchWriter buffer size | [default](https://github.com/apache/accumulo/blob/master/core/src/main/java/org/apache/accumulo/core/client/BatchWriterConfig.java#L40)
timely.metric.age.off.days | The number of days to keep metrics | 7
timely.cors.allow.any.origin | Allow any origin in cross origin requests (true/false) | false
timely.cors.allow.null.origin | Allow null origins in cross origin requests (true/false) | false
timely.cors.allowed.origins | List of allowed origins in cross origin requests (can be null or comma separated list)
timely.cors.allowed.methods | List of allowed methods for cross origin requests | <ul><li>DELETE</li><li>GET</li><li>HEAD</li><li>OPTIONS</li><li>PUT</li><li>POST</li></ul>
timely.cors.allowed.headers | Comma separated list of allowed HTTP headers for cross origin requests | content-type
timely.cors.allow.credentials | Allow credentials to be passed in cross origin requests (true/false) | true
timely.scanner.threads | Number of BatchScanner threads to be used in a query | 4
timely.metrics.report.tags.ignored | Comma separated list of tags which will not be shown in the /api/metrics response |
timely.meta.cache.expiration.minutes | Number of minutes after which unaccessed meta information will be purged from the meta cache | 60
timely.meta.cache.initial.capacity | Initial capacity of the meta cache | 2000
timely.meta.cache.max.capacity | Maximum capacity of the meta cache | 10000
timely.ssl.certificate.file | Public certificate to use for the Timely server (x509 pem format) |
timely.ssl.key.file | Private key to use for the Timely server (in pkcs8 format) |
timely.ssl.key.pass | Password to the private key |
timely.ssl.use.generated.keypair | Use a generated certificate/key pair - useful for testing | false
timely.ssl.trust.store.file | Certificate trust store (a concatenated list of trusted CA x509 pem certificates) |
timely.ssl.use.openssl | Use OpenSSL (vs JDK SSL) | true
timely.ssl.use.ciphers | List of allowed SSL ciphers | see Configuration.java
timely.session.max.age | Setting for max age of session cookie (in seconds) | 86400
timely.http.host | Address for the Timely server, used for the session cookie domain |
timely.allow.anonymous.access | Allow anonymous access | false
timely.visibility.cache.expiration.minutes | Column Visibility Cache Expiration (minutes) | 60
timely.visibility.cache.initial.capacity | Column Visibility Cache Initial Capacity | 2000
timely.visibility.cache.max.capacity | Column Visibility Cache Max Capacity | 10000
timely.web.socket.timeout | Number of seconds with no client ping response before closing subscription | 60
timely.ws.subscription.lag | Number of seconds that subscriptions should lag to account for latency | 120
timely.http.redirect.path | Path to use for HTTP to HTTPS redirect | /secure-me
timely.hsts.max.age | HTTP Strict Transport Security max age (in seconds) | 604800

## [Security] (#security)
---

Timely allows users to optionally label their data using Accumulo column visibility expressions. To enable this feature, users should put the expressions in a tag named `viz`. Timely will [flatten] (http://accumulo.apache.org/1.7/apidocs/org/apache/accumulo/core/security/ColumnVisibility.html#flatten%28%29) this expression and store it in the column visibility of the data point in the metrics table. Column visibilities are *not* stored in the meta table, so anyone can see metric names, tag names, and tag values.

Timely provides HTTPS access to the query endpoints. It is possible to allow anonymous access to these endpoints which will allow anyone to see unlabeled data. Timely uses Spring Security to configure user authentication and user role information. Users must call the `/login` endpoint for authentication and Timely will respond by setting a HTTP cookie with a session id. The response to a successful login will be a temporary redirect (HTTP 307) to the configured address for the Grafana server. Grafana itself must be configured to use https for this to work properly. For more information see the [Quick Start] (quick-start/QUICK_START.md) documentation.

## Tuning
---

1. Each thread in the Timely server that is used for processing TCP put operations has its own BatchWriter. Each BatchWriter honors the `timely.write.latency` and `timely.write.threads` configuration property, but the buffer size for each BatchWriter is `timely.write.buffer.size` divided by the number of threads. For example, if you have 8 threads processing put operations and the following settings, then you will have 8 BatchWriters each using 2 threads with a 30s latency and a maximum buffer size of 128M:

   ```
   timely.write.latency=30s
   timely.write.threads=2
   timely.write.buffer.size=1G
   ``` 

2. The `timely.scanner.threads` property is used for BatchScanners on a per query basis. If you set this to 32 and have 8 threads processing HTTP operations, then you might have 256 threads concurrently querying your tablet servers. Be sure to set your ulimits appropriately.

3. The Timely server contains an object called the meta cache, which is a cache of the keys that would be inserted into the meta table if they did not already exist. This cache is used to reduce the insert load of duplicate keys into the meta table and to serve up data to the `/api/metrics` endpoint. The meta cache is an object that supports eviction based on last access time and can be tuned with the `timely.meta.cache.*`  properties.


## NOTES
---

1. Not shown in the examples is the encoding of the row and value. You can apply the Timely formatter to your metrics table using the Accumulo shell command:
   ```
   config -t timely.metrics -s table.formatter=timely.util.TimelyMetricsFormatter
   ```

2. You can create split points for your metrics table based on the 'm' column family in the meta table.

3. You can lower the `table.scan.max.memory` property on your metrics table in an attempt to get data back faster from the tablet servers.

4. If you don't mind losing some metric data in the event of an Accumulo tablet server death, you can set the `table.walog.enabled` property to false and the `table.durability` property to none on your metrics table. This should speed up ingest a little.

