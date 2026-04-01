package timely.collectd.plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.collectd.api.ValueList;

public class MetricData {

    public static final String COUNTER = "COUNTER";
    public static final String GAUGE = "GAUGE";
    public static final String DERIVE = "DERIVE";
    public static final String ABSOLUTE = "ABSOLUTE";

    private Long timestamp;
    private String host;
    private String plugin;
    private String pluginInstance;
    private String type;
    private String typeInstance;
    private List<Pair<Double,String>> valuePairs = new ArrayList<>();

    public MetricData(Long timestamp, String host, String plugin, String pluginInstance, String type, String typeInstance, List<Double> values,
                    List<String> valueTypes) {
        this.timestamp = timestamp;
        this.host = host;
        this.plugin = plugin;
        this.pluginInstance = pluginInstance;
        this.type = type;
        this.typeInstance = typeInstance;
        for (int x = 0; x < values.size(); x++) {
            Double value = values.get(x);
            String valueType = valueTypes.size() > x ? valueTypes.get(x) : "GAUGE";
            valuePairs.add(Pair.of(value, valueType));
        }
    }

    public MetricData(ValueList vl) {
        this.timestamp = vl.getTime();
        this.host = vl.getHost();
        this.plugin = vl.getPlugin();
        this.pluginInstance = vl.getPluginInstance();
        this.type = vl.getType();
        this.typeInstance = vl.getTypeInstance();
        List<Number> valueList = vl.getValues();
        List<String> valueTypes = vl.getDataSet().getDataSources().stream().map(ds -> convertType(ds.getType())).collect(Collectors.toList());
        for (int x = 0; x < vl.getValues().size(); x++) {
            Number n = valueList.get(x);
            String type = valueTypes.size() > x ? valueTypes.get(x) : "GAUGE";
            valuePairs.add(Pair.of(n.doubleValue(), type));
        }
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPlugin() {
        return plugin;
    }

    public void setPlugin(String plugin) {
        this.plugin = plugin;
    }

    public String getPluginInstance() {
        return pluginInstance;
    }

    public void setPluginInstance(String pluginInstance) {
        this.pluginInstance = pluginInstance;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTypeInstance() {
        return typeInstance;
    }

    public void setTypeInstance(String typeInstance) {
        this.typeInstance = typeInstance;
    }

    public List<Pair<Double,String>> getValuePairs() {
        return valuePairs;
    }

    public void setValuePairs(List<Pair<Double,String>> valuePairs) {
        this.valuePairs = valuePairs;
    }

    public static String convertType(int type) {
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

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("timestamp", timestamp).append("host", host).append("plugin", plugin).append("pluginInstance", pluginInstance)
                        .append("type", type).append("typeInstance", typeInstance).append("valuePairs", valuePairs).toString();
    }
}
