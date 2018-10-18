package timely.api.response.timeseries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class QueryResponse {

    private String metric;
    private Map<String, String> tags = new HashMap<>();
    private List<String> aggregatedTags = new ArrayList<>();
    private Map<String, Object> dps = new LinkedHashMap<>();

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public void putTag(String key, String value) {
        this.tags.put(key, value);
    }

    public List<String> getAggregatedTags() {
        return aggregatedTags;
    }

    public void setAggregatedTags(List<String> aggregatedTags) {
        this.aggregatedTags = aggregatedTags;
    }

    public void addAggregatedTag(String tag) {
        this.aggregatedTags.add(tag);
    }

    public Map<String, Object> getDps() {
        return dps;
    }

    public void setDps(Map<String, Object> dps) {
        this.dps = dps;
    }

    public void putDps(String key, Object value) {
        this.dps.put(key, value);
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("metric", this.metric);
        tsb.append("tags", this.tags);
        tsb.append("aggregatedTags", this.aggregatedTags);
        tsb.append("dps", this.dps);
        return tsb.toString();
    }

}
