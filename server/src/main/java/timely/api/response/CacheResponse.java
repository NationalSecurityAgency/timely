package timely.api.response;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;

public class CacheResponse {

    private Long oldestTimestamp;
    private Long newestTimestamp;
    private List<String> metrics;

    public Long getOldestTimestamp() {
        return oldestTimestamp;
    }

    public void setOldestTimestamp(Long oldestTimestamp) {
        this.oldestTimestamp = oldestTimestamp;
    }

    public Long getNewestTimestamp() {
        return newestTimestamp;
    }

    public void setNewestTimestamp(Long newestTimestamp) {
        this.newestTimestamp = newestTimestamp;
    }

    public List<String> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<String> metrics) {
        this.metrics = metrics;
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("oldestTimestamp", this.oldestTimestamp);
        tsb.append("newestTimestamp", this.newestTimestamp);
        tsb.append("metrics", this.metrics);
        return tsb.toString();
    }

}