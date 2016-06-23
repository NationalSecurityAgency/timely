package timely.api.query.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import timely.api.AuthenticatedRequest;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class QueryRequest extends AuthenticatedRequest {

    public static class RateOption {

        private boolean counter = false;
        private long counterMax = Long.MAX_VALUE;
        private long resetValue = 0;

        public RateOption() {
        }

        public RateOption(String options) {
            String[] parts = options.split(",");
            if (parts.length == 1) {
                if (!parts[0].equals("")) {
                    this.counter = Boolean.valueOf(parts[0]);
                }
            }
            if (parts.length == 2) {
                if (!parts[1].equals("")) {
                    this.counterMax = Long.parseLong(parts[1]);
                }
            }
            if (parts.length == 3) {
                if (!parts[2].equals("")) {
                    this.resetValue = Long.parseLong(parts[2]);
                }
            }
        }

        public boolean isCounter() {

            return counter;
        }

        public void setCounter(boolean counter) {

            this.counter = counter;
        }

        public long getCounterMax() {

            return counterMax;
        }

        public void setCounterMax(long counterMax) {

            this.counterMax = counterMax;
        }

        public long getResetValue() {

            return resetValue;
        }

        public void setResetValue(long resetValue) {

            this.resetValue = resetValue;
        }

        @Override
        public String toString() {
            ToStringBuilder tsb = new ToStringBuilder(this);
            tsb.append("counter", counter);
            tsb.append("counterMax", counterMax);
            tsb.append("resetValue", resetValue);
            return tsb.toString();
        }

        @Override
        public int hashCode() {
            HashCodeBuilder hcb = new HashCodeBuilder();
            hcb.append(counter);
            hcb.append(counterMax);
            hcb.append(resetValue);
            return hcb.toHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (null == obj) {
                return false;
            }
            if (this == obj) {
                return false;
            }
            if (obj instanceof RateOption) {
                RateOption other = (RateOption) obj;
                EqualsBuilder eq = new EqualsBuilder();
                eq.append(this.counter, other.counter);
                eq.append(this.counterMax, other.counterMax);
                eq.append(this.resetValue, other.resetValue);
                return eq.isEquals();
            } else {
                return false;
            }
        }

    }

    public static class Filter {

        private String type;
        private String tagk;
        private String filter;
        private boolean groupBy = false;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getTagk() {
            return tagk;
        }

        public void setTagk(String tagk) {
            this.tagk = tagk;
        }

        public String getFilter() {
            return filter;
        }

        public void setFilter(String filter) {
            this.filter = filter;
        }

        public boolean isGroupBy() {
            return groupBy;
        }

        public void setGroupBy(boolean groupBy) {
            this.groupBy = groupBy;
        }

        @Override
        public String toString() {
            ToStringBuilder tsb = new ToStringBuilder(this);
            tsb.append("tagk", tagk);
            tsb.append("type", type);
            tsb.append("filter", filter);
            tsb.append("groupBy", groupBy);
            return tsb.toString();
        }

        @Override
        public int hashCode() {
            HashCodeBuilder hcb = new HashCodeBuilder();
            hcb.append(filter);
            hcb.append(tagk);
            hcb.append(filter);
            hcb.append(groupBy);
            return hcb.toHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (null == obj) {
                return false;
            }
            if (this == obj) {
                return false;
            }
            if (obj instanceof Filter) {
                Filter other = (Filter) obj;
                EqualsBuilder eq = new EqualsBuilder();
                eq.append(this.tagk, other.tagk);
                eq.append(this.type, other.type);
                eq.append(this.filter, other.filter);
                eq.append(this.groupBy, other.groupBy);
                return eq.isEquals();
            } else {
                return false;
            }
        }
    }

    public static class SubQuery {

        private String aggregator;
        private String metric;
        private boolean rate = false;
        private RateOption rateOptions = null;
        private Optional<String> downsample = Optional.empty();
        private Map<String, String> tags = new LinkedHashMap<>();
        private Collection<Filter> filters = new ArrayList<>();
        private Collection<String> tsuids = new ArrayList<>();

        public String getAggregator() {
            return aggregator;
        }

        public void setAggregator(String aggregator) {
            this.aggregator = aggregator;
        }

        public String getMetric() {
            return metric;
        }

        public void setMetric(String metric) {
            this.metric = metric;
        }

        public boolean isRate() {
            return rate;
        }

        public void setRate(boolean rate) {
            this.rate = rate;
        }

        public RateOption getRateOptions() {
            return rateOptions;
        }

        public void setRateOptions(RateOption rateOptions) {
            this.rateOptions = rateOptions;
        }

        public Optional<String> getDownsample() {
            return downsample;
        }

        public void setDownsample(Optional<String> downsample) {
            this.downsample = downsample;
        }

        public Map<String, String> getTags() {
            return tags;
        }

        public void setTags(Map<String, String> tags) {
            this.tags = tags;
        }

        public void addTag(String key, String value) {
            this.tags.put(key, value);
        }

        public Collection<Filter> getFilters() {
            return filters;
        }

        public void setFilters(Collection<Filter> filters) {
            this.filters = filters;
        }

        public void addFilter(Filter f) {
            this.filters.add(f);
        }

        public Collection<String> getTsuids() {
            return tsuids;
        }

        public void setTsuids(Collection<String> tsuids) {
            this.tsuids = tsuids;
        }

        @JsonIgnore
        public boolean isTsuidQuery() {
            return (this.tsuids.size() > 0);
        }

        public void addTsuid(String tsuid) {
            this.tsuids.add(tsuid);
        }

        @JsonIgnore
        public boolean isMetricQuery() {
            return (null != this.metric);
        }

        @Override
        public String toString() {
            ToStringBuilder tsb = new ToStringBuilder(this);
            tsb.append("aggregator", aggregator);
            tsb.append("metric", metric);
            tsb.append("rate", tags);
            tsb.append("rateOptions", rateOptions);
            tsb.append("downsample", downsample);
            tsb.append("tags", tags);
            tsb.append("filters", filters);
            tsb.append("tsuids", tsuids);
            return tsb.toString();
        }

        @Override
        public int hashCode() {
            HashCodeBuilder hcb = new HashCodeBuilder();
            hcb.append(aggregator);
            hcb.append(downsample);
            hcb.append(filters);
            hcb.append(metric);
            hcb.append(rate);
            hcb.append(rateOptions);
            hcb.append(tags);
            hcb.append(tsuids);
            return hcb.toHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (null == obj) {
                return false;
            }
            if (this == obj) {
                return false;
            }
            if (obj instanceof SubQuery) {
                SubQuery other = (SubQuery) obj;
                EqualsBuilder eq = new EqualsBuilder();
                eq.append(this.aggregator, other.aggregator);
                eq.append(this.downsample, other.downsample);
                eq.append(this.filters, other.filters);
                eq.append(this.metric, other.metric);
                eq.append(this.rate, other.rate);
                eq.append(this.rateOptions, other.rateOptions);
                eq.append(this.tags, other.tags);
                eq.append(this.tsuids, other.tsuids);
                return eq.isEquals();
            } else {
                return false;
            }
        }
    }

    private long start = 0;
    private long end = System.currentTimeMillis();
    private Collection<SubQuery> queries = new ArrayList<>();
    private boolean noAnnotations = false;
    private boolean globalAnnotations = false;
    private boolean msResolution = false;
    private boolean showSummary = false;
    private boolean showQuery = false;
    private boolean delete = false;

    public boolean isGlobalAnnotations() {
        return globalAnnotations;
    }

    public void setGlobalAnnotations(boolean globalAnnotations) {
        this.globalAnnotations = globalAnnotations;
    }

    public boolean isShowQuery() {
        return showQuery;
    }

    public void setShowQuery(boolean showQuery) {
        this.showQuery = showQuery;
    }

    public boolean isMsResolution() {
        return msResolution;
    }

    public void setMsResolution(boolean msResolution) {
        this.msResolution = msResolution;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public Collection<SubQuery> getQueries() {
        return queries;
    }

    public void setQueries(Collection<SubQuery> queries) {
        this.queries = queries;
    }

    public void addQuery(SubQuery query) {
        this.queries.add(query);
    }

    @Override
    public void validate() {
        super.validate();
        if (queries.size() == 0) {
            throw new IllegalArgumentException("No query specified.");
        }
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("start", start);
        tsb.append("end", end);
        tsb.append("queries", queries);
        tsb.append("noAnnotations", noAnnotations);
        tsb.append("globalAnnotations", globalAnnotations);
        tsb.append("msResolution", msResolution);
        tsb.append("showSummary", showSummary);
        tsb.append("showQuery", showQuery);
        tsb.append("delete", delete);
        return tsb.toString();
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(start);
        hcb.append(end);
        hcb.append(queries);
        hcb.append(noAnnotations);
        hcb.append(globalAnnotations);
        hcb.append(msResolution);
        hcb.append(showSummary);
        hcb.append(showQuery);
        hcb.append(delete);
        return hcb.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (this == obj) {
            return false;
        }
        if (obj instanceof QueryRequest) {
            QueryRequest other = (QueryRequest) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(this.start, other.start);
            eq.append(this.end, other.end);
            eq.append(this.queries, other.queries);
            eq.append(this.noAnnotations, other.noAnnotations);
            eq.append(this.globalAnnotations, other.globalAnnotations);
            eq.append(this.msResolution, other.msResolution);
            eq.append(this.showSummary, other.showSummary);
            eq.append(this.showQuery, other.showQuery);
            eq.append(this.delete, other.delete);
            return eq.isEquals();
        } else {
            return false;
        }
    }
}
