package timely.api.response.timeseries;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

@JsonPropertyOrder({ "type", "metric", "tags", "limit", "time", "totalResults", "results" })
public class SearchLookupResponse {

    @JsonPropertyOrder({ "tags", "metric", "tsuid" })
    public static class Result {

        private Map<String, String> tags = new LinkedHashMap<>();
        private String metric;
        private String tsuid;

        public Map<String, String> getTags() {
            return tags;
        }

        public void setTags(Map<String, String> tags) {
            this.tags = tags;
        }

        public void putTag(String key, String value) {
            this.tags.put(key, value);
        }

        public String getMetric() {
            return metric;
        }

        public void setMetric(String metric) {
            this.metric = metric;
        }

        public String getTsuid() {
            return tsuid;
        }

        public void setTsuid(String tsuid) {
            this.tsuid = tsuid;
        }

        @Override
        public boolean equals(Object obj) {
            if (null == obj) {
                return false;
            }
            if (this == obj) {
                return true;
            }
            if (obj instanceof Result) {
                Result other = (Result) obj;
                EqualsBuilder builder = new EqualsBuilder();
                builder.append(this.metric, other.metric);
                builder.append(this.tsuid, other.tsuid);
                builder.append(this.tags, other.tags);
                return builder.isEquals();
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            HashCodeBuilder hcb = new HashCodeBuilder();
            hcb.append(this.metric);
            hcb.append(this.tsuid);
            hcb.append(this.tags);
            return hcb.toHashCode();
        }

    }

    private String type;
    private String metric;
    private Map<String, String> tags = new LinkedHashMap<>();
    private int limit;
    private int time;
    private int totalResults;
    private List<Result> results = new ArrayList<>();
    private int startIndex = 0; // Does not change, here for serialization

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

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

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public int getTotalResults() {
        return totalResults;
    }

    public void setTotalResults(int totalResults) {
        this.totalResults = totalResults;
    }

    public List<Result> getResults() {
        return results;
    }

    public void setResults(List<Result> results) {
        this.results = results;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof SearchLookupResponse) {
            SearchLookupResponse other = (SearchLookupResponse) obj;
            EqualsBuilder builder = new EqualsBuilder();
            builder.append(this.type, other.type);
            builder.append(this.metric, other.metric);
            builder.append(this.time, other.time);
            builder.append(this.totalResults, other.totalResults);
            builder.append(this.tags, other.tags);
            builder.append(this.results, other.results);
            builder.append(this.startIndex, other.startIndex);
            builder.append(this.limit, other.limit);
            return builder.isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(this.type);
        hcb.append(this.metric);
        hcb.append(this.time);
        hcb.append(this.totalResults);
        hcb.append(this.tags);
        hcb.append(this.results);
        hcb.append(this.startIndex);
        hcb.append(this.limit);
        return hcb.toHashCode();
    }

}
