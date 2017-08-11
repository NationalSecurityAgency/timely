# Release Notes

## 0.0.5

### FEATURES

- Made changes and tested with Grafana 4.4.3 (alerting not supported yet)
- Updated provided dashboards

### NOTES

- Build modified to *not* build collectd modules by default, use the `collectd` maven profile at build time

## 0.0.4

### FEATURES

- Added a Timely client library that contains Java code for interacting with the Timely API. Not all operations are finished.
- Added an analytics module for writing Apache Flink jobs
- Exposed Subscription Scanner configuration settings in the Timely configuration

### PERFORMANCE

- AgeOff performance is greatly improved with a new iterator
- Added netty-transport-native-epoll as an optional runtime dependency

### NOTES

- Timely has not been tested with Grafana 4.x
- The aggregation function (not the downsample aggregration function) in Grafana did not work prior to this release, it now works.

## 0.0.3

### FEATURES
- Configuration file format changed to YAML due to use of Spring Configuration and Spring Boot
- Removed Double Lexicoder in Accumulo Value, requires removal of old data
- Added support for put operations in binary form using Google FlatBuffers
- Updated docker image
- Ageoff (in days) for individual metrics now possible, default value supported. Ageoff iterators are removed and re-applied during startup.
- Timely works with Accumulo 1.7 and 1.8
- Added support for PUT in text and binary form over UDP

### PERFORMANCE
- Modified code to use one ageoff iterator instead of a stack of them
- Removed interpolation code and code that handles counters in a special manner. This should be done in the client.
- Moved rate calculation to the tablet server by using an iterator that groups time series together and then applies a filter

### NOTES

1. This list of jars that are needed on the tablet server has increased and now includes:

 * commons-lang3
 * commons-collections4
 * guava
 * timely-client
 * timely-server

Additionally, the Accumulo classloader has to be configured for post-delegation given that the version of Guava that we depend on is
newer than what Accumulo and Hadoop uses.

2. The counter checkbox in the Grafana U/I does nothing. We are looking to fix this up in the next release.
