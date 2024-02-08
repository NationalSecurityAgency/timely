package timely.collectd.plugin;

import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.collectd.api.Collectd;
import org.collectd.api.DataSource;
import org.collectd.api.OConfigItem;
import org.collectd.api.ValueList;

public abstract class CollectDPluginParent {

    private static final String PUT = "put {0} {1} {2}{3}\n";
    private static final Pattern HADOOP_STATSD_PATTERN = Pattern.compile("([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)");
    private static final String INSTANCE = " instance=";
    private static final String NAME = " name=";
    private static final String SAMPLE = " sample=";
    private static final String CODE = " code=";
    private static final String PERIOD = ".";
    private static final String SAMPLE_TYPE = " sampleType=";
    private static final String COUNTER = "COUNTER";
    private static final String GAUGE = "GAUGE";
    private static final String DERIVE = "DERIVE";
    private static final String ABSOLUTE = "ABSOLUTE";
    private static final Pattern NSQ_PATTERN1 = Pattern.compile("([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)");
    private static final Pattern NSQ_PATTERN2 = Pattern.compile("([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)");
    private static final Pattern NSQ_PATTERN3 = Pattern.compile("([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_#]+)\\.([\\w-_]+)");
    private static final Pattern ETHSTAT_PATTERN = Pattern.compile("([\\w-_]+)_queue_([\\w-_]+)_([\\w-_]+)");
    private static final String STATSD_PREFIX = "statsd";
    private static final String NSQ_PREFIX = STATSD_PREFIX + ".nsq.";
    private static final Pattern HAPROXY_PATTERN = Pattern.compile("\\[([\\w-_=]+),([\\w-_=]+)\\]");

    private final SMARTCodeMapping smart = new SMARTCodeMapping();

    protected Set<String> addlTags = new HashSet<>();

    public int config(OConfigItem config) {
        for (OConfigItem child : config.getChildren()) {
            switch (child.getKey()) {
                case "Tags":
                case "tags":
                    String[] additionalTags = child.getValues().get(0).getString().split(",");
                    for (String t : additionalTags) {
                        addlTags.add(" " + t.trim());
                    }
                default:
            }
        }
        return 0;
    }

    public abstract void write(String metric, OutputStream out);

    public void process(ValueList vl, OutputStream out) {
        debugValueList(vl);

        StringBuilder metric = new StringBuilder();
        Long timestamp = vl.getTime();
        StringBuilder tags = new StringBuilder();
        String host = vl.getHost();
        int idx = host.indexOf(PERIOD);
        if (-1 != idx) {
            tags.append(" host=").append(host.substring(0, idx));
        } else {
            tags.append(" host=").append(host);
        }
        int nIdx = host.indexOf('n');
        if (-1 != nIdx) {
            tags.append(" rack=").append(host.substring(0, nIdx));
        }
        for (String tag : addlTags) {
            tags.append(tag);
        }
        if (vl.getPlugin().equals(STATSD_PREFIX)) {
            Matcher m = HADOOP_STATSD_PATTERN.matcher(vl.getTypeInstance());
            Matcher n1 = NSQ_PATTERN1.matcher(vl.getTypeInstance());
            Matcher n2 = NSQ_PATTERN2.matcher(vl.getTypeInstance());
            Matcher n3 = NSQ_PATTERN3.matcher(vl.getTypeInstance());
            String instance = null;
            if (m.matches() && !vl.getTypeInstance().startsWith("nsq")) {
                // Here we are processing the statsd metrics coming from the
                // Hadoop Metrics2 StatsDSink without the host name.
                // The format of metric is:
                // serviceName.contextName.recordName.metricName. The recordName
                // is typically duplicative and is dropped here. The serviceName
                // is used as the instance.
                metric.append(STATSD_PREFIX).append(PERIOD).append(m.group(2)).append(PERIOD).append(m.group(4));
                instance = m.group(1);
            } else if (n1.matches() && vl.getTypeInstance().startsWith("nsq")) {
                metric.append(NSQ_PREFIX).append(n1.group(2)).append(PERIOD).append(n1.group(3));
            } else if (n2.matches() && vl.getTypeInstance().startsWith("nsq")) {
                metric.append(NSQ_PREFIX).append(n2.group(2)).append(PERIOD).append(n2.group(4));
                instance = n2.group(3);
            } else if (n3.matches() && vl.getTypeInstance().startsWith("nsq")) {
                metric.append(NSQ_PREFIX).append(n3.group(4)).append(PERIOD).append(n3.group(6));
                instance = n3.group(5);
            } else {
                // Handle StatsD metrics of unknown formats. If there is a
                // period in the metric name, use everything up to that as
                // the instance.
                int period = vl.getTypeInstance().indexOf('.');
                if (-1 == period) {
                    metric.append(STATSD_PREFIX).append(PERIOD).append(vl.getTypeInstance());
                } else {
                    instance = vl.getTypeInstance().substring(0, period);
                    metric.append(STATSD_PREFIX).append(PERIOD).append(vl.getTypeInstance().substring(period + 1));
                }
            }
            timestamp = vl.getTime();
            if (null != instance) {
                tags.append(INSTANCE).append(instance);
            }
        } else if (vl.getPlugin().equals("ethstat")) {
            metric.append("sys.ethstat.");
            if (vl.getTypeInstance().contains("queue")) {
                Matcher m = ETHSTAT_PATTERN.matcher(vl.getTypeInstance());
                if (m.matches()) {
                    metric.append(m.group(1)).append("_").append(m.group(3));
                    tags.append(" queue=").append(m.group(2));
                } else {
                    metric.append(vl.getTypeInstance());
                }
            } else {
                metric.append(vl.getTypeInstance());
            }
            tags.append(INSTANCE).append(vl.getPluginInstance());
        } else if (vl.getPlugin().equals("hddtemp")) {
            metric.append("sys.hddtemp.").append(vl.getType());
            tags.append(INSTANCE).append(vl.getTypeInstance());
        } else if (vl.getPlugin().equals("smart")) {
            int code = -1;
            String name = null;
            if (vl.getTypeInstance().startsWith("attribute-")) {
                int hyphen = vl.getTypeInstance().indexOf('-');
                code = Integer.parseInt(vl.getTypeInstance().substring(hyphen + 1));
                name = smart.get(code);
            }
            if (code == -1) {
                if (notEmpty(vl.getTypeInstance())) {
                    metric.append("sys.smart.").append(vl.getTypeInstance());
                } else {
                    metric.append("sys.smart.").append(vl.getType());
                }
            } else {
                metric.append("sys.smart.").append(name);
                tags.append(CODE).append(code);
            }
            tags.append(INSTANCE).append(vl.getPluginInstance());
        } else if (vl.getPlugin().equals("sensors")) {
            String instance = "";
            if (vl.getTypeInstance().startsWith("temp")) {
                instance = vl.getTypeInstance().substring(4);
            }
            metric.append("sys.sensors.").append(vl.getType()).append(PERIOD).append(vl.getPluginInstance());
            tags.append(INSTANCE).append(instance);
        } else if (vl.getPlugin().equals("haproxy")) {
            metric.append("sys.haproxy.").append(vl.getTypeInstance());
            Matcher m = HAPROXY_PATTERN.matcher(vl.getPluginInstance());
            if (m.matches()) {
                tags.append(" ").append(m.group(1));
                tags.append(" ").append(m.group(2));
            }
        } else if (vl.getPlugin().equals("ipmi")) {
            metric.append("sys.ipmi.").append(vl.getType());
            tags.append(INSTANCE).append(vl.getTypeInstance().replaceAll(" ", "_"));
        } else if (vl.getPlugin().equals("snmp")) {
            metric.append("sys.snmp.").append(vl.getType());
            tags.append(INSTANCE).append(vl.getTypeInstance().replaceAll(" ", "_"));
        } else if (vl.getPlugin().equals("GenericJMX")) {
            metric.append("sys.").append(vl.getPlugin()).append(PERIOD).append(vl.getType()).append(PERIOD).append(vl.getTypeInstance());
            String[] pluginInstanceSplit = vl.getPluginInstance().split("-");
            if (pluginInstanceSplit.length > 0) {
                tags.append(INSTANCE).append(pluginInstanceSplit[0].replaceAll(" ", "_"));
            }
            if (pluginInstanceSplit.length > 1) {
                tags.append(NAME).append(pluginInstanceSplit[1].replaceAll(" ", "_"));
            }
        } else if (notEmpty(vl.getTypeInstance()) && notEmpty(vl.getType()) && notEmpty(vl.getPluginInstance()) && notEmpty(vl.getPlugin())) {
            metric.append("sys.").append(vl.getPlugin()).append(PERIOD).append(vl.getType()).append(PERIOD).append(vl.getTypeInstance());
            tags.append(INSTANCE).append(vl.getPluginInstance().replaceAll(" ", "_"));
        } else if (notEmpty(vl.getTypeInstance()) && notEmpty(vl.getType()) && notEmpty(vl.getPlugin())) {
            metric.append("sys.").append(vl.getPlugin()).append(PERIOD).append(vl.getType()).append(PERIOD).append(vl.getTypeInstance());
        } else if (notEmpty(vl.getType()) && notEmpty(vl.getPluginInstance()) && notEmpty(vl.getPlugin())) {
            metric.append("sys.").append(vl.getPlugin()).append(PERIOD).append(vl.getType());
            tags.append(INSTANCE).append(vl.getPluginInstance().replaceAll(" ", "_"));
        } else if (notEmpty(vl.getType()) && notEmpty(vl.getPlugin())) {
            metric.append("sys.").append(vl.getPlugin()).append(PERIOD).append(vl.getType());
        } else {
            Collectd.logWarning("Unhandled metric: " + vl.toString());
            return;
        }
        final String metricName = metric.toString().replaceAll(" ", "_");
        for (int i = 0; i < vl.getValues().size(); i++) {
            StringBuilder tagsWithSample = new StringBuilder(tags.toString());
            String sampleName = vl.getDataSet().getDataSources().get(i).getName();
            int type = vl.getDataSet().getDataSources().get(i).getType();
            String sampleType = convertType(type);
            if (null != sampleName) {
                tagsWithSample.append(SAMPLE).append(sampleName);
            }
            if (null != sampleType) {
                tagsWithSample.append(SAMPLE_TYPE).append(sampleType);
            }
            Double value = vl.getValues().get(i).doubleValue();
            String datapoint = MessageFormat.format(PUT, metricName, timestamp.toString(), value.toString(), tagsWithSample.toString());
            Collectd.logDebug("Writing: " + datapoint);
            write(datapoint, out);
        }
    }

    private void debugValueList(ValueList vl) {
        Collectd.logDebug("Input: " + vl.toString());
        Collectd.logDebug("Plugin: " + vl.getPlugin());
        Collectd.logDebug("PluginInstance: " + vl.getPluginInstance());
        Collectd.logDebug("Type: " + vl.getType());
        Collectd.logDebug("TypeInstance: " + vl.getTypeInstance());
        for (int i = 0; i < vl.getValues().size(); i++) {
            Number value = vl.getValues().get(i);
            DataSource ds = vl.getDataSet().getDataSources().get(i);
            Collectd.logDebug(convertType(ds.getType()) + " " + ds.getName() + " = " + value);
        }
    }

    private String convertType(int type) {
        String result = null;
        switch (type) {
            case 0:
                result = COUNTER;
                break;
            case 1:
                result = GAUGE;
                break;
            case 2:
                result = DERIVE;
                break;
            case 3:
                result = ABSOLUTE;
                break;
            default:
                result = GAUGE;
                break;
        }
        return result;
    }

    private boolean notEmpty(String arg) {
        return (null != arg) && !(arg.equals(""));
    }

}
