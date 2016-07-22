# Plugins for CollectD

The source tree includes plugins for [CollectD] (http://collectd.org/), an agent based system statistics collector. The CollectD plugins parse and transform the metrics and sends them to Timely.

## CollectD plugin to send data to Timely

```python
LoadPlugin java
<Plugin java>
        JVMArg "-verbose:jni"
        JVMArg "-Djava.class.path=/usr/share/collectd/java/collectd-api.jar:<path_to_jar>/collectd-timely-plugin.jar"
        JVMArg "-Xms128m"
        JVMArg "-Xmx128m"
        LoadPlugin "timely.collectd.plugin.WriteTimelyPlugin"
        <Plugin "timely.collectd.plugin.WriteTimelyPlugin">
          Host "<TimelyHost>"
          Port "<TimelyPutPort>"
          Tags "<comma separated list of additional key=value pairs>"
        </Plugin>
</Plugin>
```

To build, run `mvn clean package` in the collectd-timely-plugin directory. Place the resulting jar file somewhere on the local filesystem and the plugin configuration to the right to the collectd configuration file.

## CollectD plugin to send data to [NSQ] (http://nsq.io/)

```python
LoadPlugin java
<Plugin java>
        JVMArg "-verbose:jni"
        JVMArg "-Djava.class.path=/usr/share/collectd/java/collectd-api.jar:<path_to_jar>/collectd-nsq-plugin.jar"
        JVMArg "-Xms128m"
        JVMArg "-Xmx128m"
        LoadPlugin "timely.collectd.plugin.WriteNSQPlugin"
        <Plugin "timely.collectd.plugin.WriteNSQPlugin">
          Host "<NSQHost[,NSQHost]>"
          Port "<NSQHttpPort>"
          Tags "<comma separated list of additional key=value pairs>"
        </Plugin>
</Plugin>
```

To build, run `mvn clean package` in the collectd-nsq-plugin directory. Place the resulting jar file somewhere on the local filesystem and add the plugin configuration to the right to the collectd configuration file.

## Collecting Metrics from Hadoop and Accumulo

CollectD contains [StatsD] (https://github.com/etsy/statsd) plugin that listens on a configured port for UDP traffic in the StatsD protocol. More recent versions of Hadoop contain a StatsD [sink] (https://issues.apache.org/jira/browse/HADOOP-12360) for the Hadoop [Metrics2] (http://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/Metrics.html) framework. Accumulo also uses the Hadoop Metrics2 framework and when configured correctly can emit its metrics via the same mechanism.

### Configuring Hadoop to use the StatsD sink


```python
*.sink.statsd.class=org.apache.hadoop.metrics2.sink.StatsDSink
*.sink.statsd.period=60
namenode.sink.statsd.server.host=127.0.0.1
namenode.sink.statsd.server.port=8125
namenode.sink.statsd.skip.hostname=true
namenode.sink.statsd.service.name=NameNode
datanode.sink.statsd.server.host=127.0.0.1
datanode.sink.statsd.server.port=8125
datanode.sink.statsd.skip.hostname=true
datanode.sink.statsd.service.name=DataNode
resourcemanager.sink.statsd.server.host=127.0.0.1
resourcemanager.sink.statsd.server.port=8125
resourcemanager.sink.statsd.skip.hostname=true
resourcemanager.sink.statsd.service.name=ResourceManager
nodemanager.sink.statsd.server.host=127.0.0.1
nodemanager.sink.statsd.server.port=8125
nodemanager.sink.statsd.skip.hostname=true
nodemanager.sink.statsd.service.name=NodeManager
mrappmaster.sink.statsd.server.host=127.0.0.1
mrappmaster.sink.statsd.server.port=8125
mrappmaster.sink.statsd.skip.hostname=true
mrappmaster.sink.statsd.service.name=MRAppMaster
jobhistoryserver.sink.statsd.server.host=127.0.0.1
jobhistoryserver.sink.statsd.server.port=8125
jobhistoryserver.sink.statsd.skip.hostname=true
jobhistoryserver.sink.statsd.service.name=JobHistoryServer
maptask.sink.statsd.server.host=127.0.0.1
maptask.sink.statsd.server.port=8125
maptask.sink.statsd.skip.hostname=true
maptask.sink.statsd.service.name=MapTask
reducetask.sink.statsd.server.host=127.0.0.1
reducetask.sink.statsd.server.port=8125
reducetask.sink.statsd.skip.hostname=true
reducetask.sink.statsd.service.name=ReduceTask
```

Uncomment and configure the `*.period` property, then append the content to the right to the hadoop-metrics2.properties file.

### Configuring Accumulo to use the StatsD sink

```python
accumulo.sink.statsd-tserver.class=org.apache.hadoop.metrics2.sink.StatsDSink
accumulo.sink.statsd-tserver.server.host=127.0.0.1
accumulo.sink.statsd-tserver.server.port=8125
accumulo.sink.statsd-tserver.skip.hostname=true
accumulo.sink.statsd-tserver.service.name=TabletServer
accumulo.sink.statsd-master.class=org.apache.hadoop.metrics2.sink.StatsDSink
accumulo.sink.statsd-master.server.host=127.0.0.1
accumulo.sink.statsd-master.server.port=8125
accumulo.sink.statsd-master.skip.hostname=true
accumulo.sink.statsd-master.service.name=Master
accumulo.sink.statsd-thrift.class=org.apache.hadoop.metrics2.sink.StatsDSink
accumulo.sink.statsd-thrift.server.host=127.0.0.1
accumulo.sink.statsd-thrift.server.port=8125
accumulo.sink.statsd-thrift.skip.hostname=true
accumulo.sink.statsd-thrift.service.name=Thrift
```

Uncomment and configure the `*.period` property, then append the content to the right to the hadoop-metrics2-accumulo.properties file.

