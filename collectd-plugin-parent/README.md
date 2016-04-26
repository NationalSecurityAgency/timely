# CollectD Plugin

The CollectD plugin formats metrics acquired by the other plugins and sends them to Timely. The plugin contains code for parsing and extracting information from metrics reported by some of the plugins. One special case is the metrics received by the [StatsD] (https://github.com/etsy/statsd) plugin. It supports processing of [NSQ] (http://nsq.io/) metrics by looking for StatsD metrics with the prefix 'nsq'. 

It also supports parsing of the StatsD metrics coming from the Hadoop Metrics2 framework (a metric with 4 components separated by periods). In this case you will want to set the skip hostname property to true and send the metrics from Hadoop to a local CollectD instance that will add the hostname and other tags. 

The plugin allows for other unspecified formats to be sent as-is, except in the following case: if the StatsD metric contains a period, everything up to the period will be used in an instance tag.

For example:

"foo:1|c" will end up as the metric 'statsd.foo'

"bar.foo:1|c" will end up as the metric 'statsd.foo' with an 'instance=bar' tag.

