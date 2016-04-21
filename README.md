# Timely
---

Timely is an Accumulo backed time series database application. Currently it has only been tested with Grafana version 2.1.3 and 2.5.0.

## Building

Timely uses Maven for compiling source, running tests, and packaging distributions. Below is a set of useful Maven lifecyles that we use:
 
Command | Description
--------|------------
mvn compile | Compiles and formats the source
mvn test | Compiles and formats source and runs unit tests
mvn package | Compiles and formats source, runs unit tests, findbugs, integration tests, and creates an assembly
 
## Deployment

Untar the distribution somewhere, modify the conf/timely.properties file appropriately. Copy the timely-server and commons-lang3 jar files to your Accumulo tservers. Then, you can run bin/timely-server.sh or bin/timely-standalone.sh to run a standalone instance that uses Accumulo MiniCluster. Be aware that the standalone instance will not save your metric data across restarts and does not use the properties file. For production instances you will likely want to dial back the logging.

## API

For historical reasons we started off by reusing the parts of the OpenTSDB api that Grafana uses. We currently support the TCP put API and we have partially implemented the following HTTP operations:

    * /api/aggregators
    * /api/query
    * /apu/search/lookup
    * /api/suggest

There is also an endpoint at /api/metrics that reports the metric names and tags that have been pushed to Timely since the server was started. This could be useful in creating graphs (so you won't have to go digging in the meta table in Accumulo).

Additionally, the following constraints and assumptions guided the design

1. We need to show progress quickly
2. We started with Grafana, so we won't need our own UI
3. We already had experience with OpenTSDB, so we started with its API
4. Space is cheap, ingest and query speed are more important
5. Metrics will be kept short term
6. The number of tags used for each metric will be small \( < 10 \)
7. We can create a more native API and Grafana plugin at a later time

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

Note: Not shown in these examples is the encoding of the row and value. You can apply the Timely formatter to your metrics table using the Accumulo shell command:
```
config -t timely.metrics -s table.formatter=timely.util.TimelyMetricsFormatter
```
