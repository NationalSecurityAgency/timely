package timely.collectd.plugin;

import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.collectd.api.Collectd;
import org.collectd.api.OConfigItem;
import org.collectd.api.ValueList;

public abstract class CollectDPluginParent {

    private static final String PUT = "put {0} {1} {2} {3}";
    private static final String INSTANCE = "instance";
    private static final String NAME = "name";
    private static final String SAMPLE_TYPE = "sampleType";
    private static final String CODE = "code";
    private static final String PERIOD = ".";
    private static final String COUNTER = "COUNTER";
    private static final String GAUGE = "GAUGE";
    private static final String DERIVE = "DERIVE";
    private static final String ABSOLUTE = "ABSOLUTE";
    private static final Pattern STATSD_PATTERN_3_GROUPS = Pattern.compile("([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)");
    private static final Pattern STATSD_PATTERN_4_GROUPS = Pattern.compile("([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)");
    private static final Pattern STATSD_PATTERN_6_GROUPS = Pattern.compile("([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_]+)\\.([\\w-_#]+)\\.([\\w-_]+)");
    private static final Pattern ETHSTAT_PATTERN = Pattern.compile("([\\w-_]+)_queue_([\\w-_]+)_([\\w-_]+)");
    private static final String STATSD_PREFIX = "statsd";
    private static final String NSQ_PREFIX = STATSD_PREFIX + ".nsq.";
    private static final Pattern HAPROXY_PATTERN = Pattern.compile("\\[([\\w-_=]+),([\\w-_=]+)\\]");

    private final SMARTCodeMapping smart = new SMARTCodeMapping();
    protected Map<String,String> addlTags = new TreeMap<>();
    private boolean debug = false;

    public int config(OConfigItem config) {
        for (OConfigItem child : config.getChildren()) {
            switch (child.getKey()) {
                case "Tags":
                case "tags":
                    String tagValue = child.getValues().get(0).getString();
                    Arrays.stream(tagValue.split(",")).map(String::trim).forEach(p -> {
                        String[] pair = p.split("=");
                        if (pair.length == 2) {
                            addTag(addlTags, pair[0], pair[1]);
                        }
                    });
                    break;
                case "Debug":
                case "debug":
                    debug = child.getValues().get(0).getBoolean();
                    break;
                default:
            }
        }
        return 0;
    }

    public abstract void write(String metric, OutputStream out);

    protected void addTag(Map<String,String> m, String k, String v) {
        if (k != null && !k.isBlank() && v != null && !v.isBlank()) {
            m.put(k.trim(), v.trim());
        }
    }

    protected void addTag(Map<String,String> m, String kv) {
        if (kv != null && !kv.isBlank()) {
            String[] parts = kv.split("=");
            if (parts.length == 2 && !parts[0].isBlank() && !parts[1].isBlank()) {
                m.put(parts[0].trim(), parts[1].trim());
            }
        }
    }

    public void process(ValueList vl, OutputStream out) {
        try {
            processInternal(vl, out);
        } catch (Exception e) {
            Collectd.logError(e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    private void processInternal(ValueList vl, OutputStream out) {
        debugValueList(vl);

        StringBuilder metric = new StringBuilder();
        Long timestamp = vl.getTime();
        Map<String,String> tagMap = new TreeMap<>();
        String host = vl.getHost();
        int idx = host.indexOf(PERIOD);
        if (-1 != idx) {
            addTag(tagMap, "host", host.substring(0, idx));
        } else {
            addTag(tagMap, "host", host);
        }
        int nIdx = host.indexOf('n');
        if (-1 != nIdx) {
            addTag(tagMap, "rack", host.substring(0, nIdx));
        }
        tagMap.putAll(addlTags);

        String plugin = vl.getPlugin();
        String pluginInstance = vl.getPluginInstance();
        String type = vl.getType();
        String typeInstance = vl.getTypeInstance();

        switch (plugin) {
            case STATSD_PREFIX:
                Matcher statsd_3_groups = STATSD_PATTERN_3_GROUPS.matcher(typeInstance);
                Matcher statsd_4_groups = STATSD_PATTERN_4_GROUPS.matcher(typeInstance);
                Matcher statsd_6_groups = STATSD_PATTERN_6_GROUPS.matcher(typeInstance);
                String instance = null;
                if (!typeInstance.startsWith("nsq")) {
                    String[] parts = typeInstance.split("\\.");
                    if (parts.length % 2 == 1 && parts.length >= 3) {
                        switch(parts[0]) {
                            case "dwingest":
                                if (parts.length >= 4) {
                                    metric.append(STATSD_PREFIX).append(PERIOD).append(parts[1]).append(PERIOD).append(parts[3]);
                                    instance = parts[0];
                                }
                                break;
                            case "dwquery":
                                if (parts.length >= 4) {
                                    metric.append(STATSD_PREFIX).append(PERIOD).append(parts[1]).append(PERIOD).append(parts[3]);
                                    addTag(tagMap, "queryId", parts[0]);
                                }
                                break;
                            default:
                                // EtsyStatsD format -- metric.(tagName.tagValue)*
                                metric.append(STATSD_PREFIX).append(PERIOD).append(parts[0]);
                                for (int x = 1; x < parts.length; x += 2) {
                                    addTag(tagMap, parts[x], parts[x + 1]);
                                }
                            }
                        }
                    } else if (statsd_4_groups.matches()) {
                        // Here we are processing the statsd metrics coming from the Hadoop Metrics2 StatsDSink without the host name.
                        // The format of metric is: serviceName.contextName.recordName.metricName. The recordName is typically duplicative
                        // and is dropped here. The serviceName is used as the instance.
                        metric.append(STATSD_PREFIX).append(PERIOD).append(statsd_4_groups.group(2)).append(PERIOD).append(statsd_4_groups.group(4));
                        instance = statsd_4_groups.group(1);
                    }
                } else if (statsd_3_groups.matches()) {
                    metric.append(NSQ_PREFIX).append(statsd_3_groups.group(2)).append(PERIOD).append(statsd_3_groups.group(3));
                } else if (statsd_4_groups.matches()) {
                    metric.append(NSQ_PREFIX).append(statsd_4_groups.group(2)).append(PERIOD).append(statsd_4_groups.group(4));
                    instance = statsd_4_groups.group(3);
                } else if (statsd_6_groups.matches()) {
                    metric.append(NSQ_PREFIX).append(statsd_6_groups.group(4)).append(PERIOD).append(statsd_6_groups.group(6));
                    instance = statsd_6_groups.group(5);
                } else {
                    // Handle StatsD metrics of unknown formats. If there is a
                    // period in the metric name, use everything up to that as
                    // the instance.
                    int period = typeInstance.indexOf('.');
                    if (-1 == period) {
                        metric.append(STATSD_PREFIX).append(PERIOD).append(typeInstance);
                    } else {
                        instance = typeInstance.substring(0, period);
                        metric.append(STATSD_PREFIX).append(PERIOD).append(typeInstance.substring(period + 1));
                    }
                }
                if (null != instance) {
                    addTag(tagMap, INSTANCE, instance);
                }
                break;
            case "ethstat":
                metric.append("sys.ethstat.");
                if (typeInstance.contains("queue")) {
                    Matcher ethstat_m = ETHSTAT_PATTERN.matcher(typeInstance);
                    if (ethstat_m.matches()) {
                        metric.append(ethstat_m.group(1)).append("_").append(ethstat_m.group(3));
                        addTag(tagMap, "queue", ethstat_m.group(2));
                    } else {
                        metric.append(typeInstance);
                    }
                } else {
                    metric.append(typeInstance);
                }
                addTag(tagMap, INSTANCE, pluginInstance);
                break;
            case "hddtemp":
                metric.append("sys.hddtemp.").append(type);
                addTag(tagMap, INSTANCE, typeInstance);
                break;
            case "smart":
                int code = -1;
                String name = null;
                if (typeInstance.startsWith("attribute-")) {
                    int hyphen = typeInstance.indexOf('-');
                    code = Integer.parseInt(typeInstance.substring(hyphen + 1));
                    name = smart.get(code);
                }
                if (code == -1) {
                    if (notEmpty(typeInstance)) {
                        metric.append("sys.smart.").append(typeInstance);
                    } else {
                        metric.append("sys.smart.").append(type);
                    }
                } else {
                    metric.append("sys.smart.").append(name);
                    addTag(tagMap, CODE, Integer.toString(code));
                }
                addTag(tagMap, INSTANCE, pluginInstance);
                break;
            case "sensors":
                if (typeInstance.startsWith("temp")) {
                    addTag(tagMap, INSTANCE, typeInstance.substring(4));
                }
                metric.append("sys.sensors.").append(type).append(PERIOD).append(pluginInstance);
                break;
            case "haproxy":
                metric.append("sys.haproxy.").append(typeInstance);
                Matcher hadoop_m2 = HAPROXY_PATTERN.matcher(pluginInstance);
                if (hadoop_m2.matches()) {
                    addTag(tagMap, hadoop_m2.group(1));
                    addTag(tagMap, hadoop_m2.group(2));
                }
                break;
            case "ipmi":
            case "snmp":
                metric.append("sys.").append(plugin).append(PERIOD).append(type);
                addTag(tagMap, INSTANCE, typeInstance.replaceAll(" ", "_"));
                break;
            case "GenericJMX":
                metric.append("sys.").append(plugin).append(PERIOD).append(type).append(PERIOD).append(typeInstance);
                String[] pluginInstanceSplit = pluginInstance.split("-");
                if (pluginInstanceSplit.length > 0) {
                    addTag(tagMap, INSTANCE, pluginInstanceSplit[0].replaceAll(" ", "_"));
                }
                if (pluginInstanceSplit.length > 1) {
                    addTag(tagMap, NAME, pluginInstanceSplit[1].replaceAll(" ", "_"));
                }
            default:
                if (notEmpty(type) && notEmpty(typeInstance) && notEmpty(plugin) && notEmpty(pluginInstance)) {
                    metric.append("sys.").append(plugin).append(PERIOD).append(type).append(PERIOD).append(typeInstance);
                    addTag(tagMap, INSTANCE, pluginInstance.replaceAll(" ", "_"));
                } else if (notEmpty(type) && notEmpty(typeInstance) && notEmpty(plugin)) {
                    metric.append("sys.").append(plugin).append(PERIOD).append(type).append(PERIOD).append(typeInstance);
                } else if (notEmpty(type) && notEmpty(plugin) && notEmpty(pluginInstance)) {
                    metric.append("sys.").append(plugin).append(PERIOD).append(type);
                    addTag(tagMap, INSTANCE, pluginInstance.replaceAll(" ", "_"));
                } else if (notEmpty(type) && notEmpty(plugin)) {
                    metric.append("sys.").append(plugin).append(PERIOD).append(type);
                } else {
                    Collectd.logWarning("Unhandled metric: " + vl);
                    return;
                }
                break;
        }
        final String metricName = metric.toString().replaceAll(" ", "_");
        for (int i = 0; i < vl.getValues().size(); i++) {
            int dataSourceSampleType = vl.getDataSet().getDataSources().get(i).getType();
            String sampleType = convertType(dataSourceSampleType);
            if (null != sampleType) {
                addTag(tagMap, SAMPLE_TYPE, sampleType);
            }
            Double value = vl.getValues().get(i).doubleValue();
            String tagString = tagMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(" "));
            String datapoint = MessageFormat.format(PUT, metricName, timestamp.toString(), value.toString(), tagString);
            logDebug(String.format("Writing: %s", datapoint));
            write(datapoint + "\n", out);
        }
    }

    private void logDebug(String s) {
        // collectd must be compiled with debug enabled in order for log level debug to work
        if (debug) {
            Collectd.logInfo(s);
        } else {
            Collectd.logDebug(s);
        }
    }

    private void debugValueList(ValueList vl) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Plugin:%s", vl.getPlugin()));
            if (vl.getPluginInstance() != null && !vl.getPluginInstance().isBlank()) {
                sb.append(String.format(" PluginInstance:%s", vl.getPluginInstance()));
            }
            sb.append(String.format(" Type:%s", vl.getType()));
            sb.append(String.format(" TypeInstance:%s", vl.getTypeInstance()));
            for (int i = 0; i < vl.getValues().size(); i++) {
                Number value = vl.getValues().get(i);
                sb.append(String.format(" Value:%s", value.toString()));
            }
            logDebug(sb.toString());
        } catch (Exception e) {
            e.printStackTrace();
            logDebug(e.getMessage());
        }
    }

    private String convertType(int type) {
        switch (type) {
            case 0:
                return COUNTER;
            case 1:
                return GAUGE;
            case 2:
                return DERIVE;
            case 3:
                return ABSOLUTE;
            default:
                return GAUGE;
        }
    }

    private boolean notEmpty(String arg) {
        return (null != arg) && !(arg.equals(""));
    }

}
