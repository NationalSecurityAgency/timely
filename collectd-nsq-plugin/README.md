# Write Timely plugin for CollectD

To build, run ```mvn clean package```. Place the resulting jar file somewhere on the local filesystem and add the following to the
collectd configuration file

```
LoadPlugin java
<Plugin java>
        JVMArg "-verbose:jni"
        JVMArg "-Djava.class.path=/usr/share/collectd/java/collectd-api.jar:<path_to_jar>/collectd-nsq-plugin-0.0.1-SNAPSHOT.jar"
        JVMArg "-Xms1024m"
        JVMArg "-Xmx1024m"
        LoadPlugin "timely.collectd.plugin.WriteNSQPlugin"
        <Plugin "timely.collectd.plugin.WriteNSQPlugin">
          Host "<NSQHost[,NSQHost]>"
          Port "<NSQHttpPort>"
        </Plugin>
</Plugin>
```