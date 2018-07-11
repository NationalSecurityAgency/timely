package timely.api.request.timeseries;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import timely.api.annotation.Http;
import timely.api.annotation.WebSocket;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.HttpGetRequest;
import timely.api.request.HttpPostRequest;
import timely.api.request.WebSocketRequest;
import timely.util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;

@Http(path = "/api/query")
@WebSocket(operation = "query")
public class QueryRequest extends AuthenticatedRequest implements HttpGetRequest, HttpPostRequest, WebSocketRequest {

    public static class RateOption {

        private boolean counter = false;
        private long counterMax = 0;
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
            this.tags = (tags == null) ? new LinkedHashMap<>() : tags;
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
            tsb.append("rate", rate);
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
    private FullHttpRequest httpRequest = null;

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

    @Override
    public HttpPostRequest parseBody(String content) throws Exception {
        // Add the operation node to the json if it does not exist for proper
        // parsing
        JsonNode root = JsonUtil.getObjectMapper().readValue(content, JsonNode.class);
        JsonNode operation = root.findValue("operation");
        if (null == operation) {
            StringBuilder buf = new StringBuilder(content.length() + 10);
            buf.append("{ \"operation\" : \"query\", ");
            int open = content.indexOf("{");
            buf.append(content.substring(open + 1));
            return JsonUtil.getObjectMapper().readValue(buf.toString(), QueryRequest.class);
        }
        return JsonUtil.getObjectMapper().readValue(content, QueryRequest.class);
    }

    @Override
    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception {
        final QueryRequest query = new QueryRequest();
        query.setStart(Long.parseLong(decoder.parameters().get("start").get(0)));
        if (decoder.parameters().containsKey("end")) {
            query.setEnd(Long.parseLong(decoder.parameters().get("end").get(0)));
        }
        if (decoder.parameters().containsKey("m")) {
            decoder.parameters()
                    .get("m")
                    .forEach(m -> {
                        final SubQuery sub = new SubQuery();
                        final String[] mParts = m.split(":");
                        if (mParts.length < 2) {
                            throw new IllegalArgumentException("Too few parameters for metric query");
                        }
                        if (mParts.length > 5) {
                            throw new IllegalArgumentException("Too many parameters for metric query");
                        }
                        // Aggregator is required, it's in the first section
                            sub.setAggregator(mParts[0]);
                            // Parse the rates from the 2nd through
                            // next to last sections
                            for (int i = 1; i < mParts.length - 1; i++) {
                                if (mParts[i].startsWith("rate")) {
                                    sub.setRate(true);
                                    final RateOption options = new RateOption();
                                    if (mParts[i].equals("rate")) {
                                        sub.setRateOptions(options);
                                    } else {
                                        final String rate = mParts[i].substring(5, mParts[i].length() - 1);
                                        final String[] rateOptions = rate.split(",");
                                        for (int x = 0; x < rateOptions.length; x++) {
                                            switch (x) {
                                                case 0:
                                                    options.setCounter(rateOptions[x].endsWith("counter"));
                                                    break;
                                                case 1:
                                                    options.setCounterMax(Long.parseLong(rateOptions[x]));
                                                    break;
                                                case 2:
                                                    options.setResetValue(Long.parseLong(rateOptions[x]));
                                                    break;
                                                default:
                                            }
                                        }
                                        sub.setRateOptions(options);
                                    }
                                } else {
                                    // downsample
                                    sub.setDownsample(Optional.of(mParts[i]));
                                }
                            }
                            // Parse metric name and tags from last
                            // section
                            final String metricAndTags = mParts[mParts.length - 1];
                            final int idx = metricAndTags.indexOf('{');
                            if (-1 == idx) {
                                // metric only
                                sub.setMetric(metricAndTags);
                            } else {
                                sub.setMetric(metricAndTags.substring(0, idx));
                                // Only supporting one set of {} as we
                                // are not supporting filters right now.
                                if (!metricAndTags.endsWith("}")) {
                                    throw new IllegalArgumentException("Tag section does not end with '}'");
                                }
                                final String[] tags = metricAndTags.substring(idx).split("}");
                                if (tags.length > 0) {
                                    // Process the first set of tags, which
                                    // are groupBy
                                    final String groupingTags = tags[0];
                                    final String[] gTags = groupingTags.substring(1, groupingTags.length()).split(",");
                                    for (final String tag : gTags) {
                                        final String[] tParts = tag.split("=");
                                        final Filter f = new Filter();
                                        f.setGroupBy(true);
                                        f.setTagk(tParts[0]);
                                        f.setFilter(tParts[1]);
                                        sub.addFilter(f);
                                    }
                                    if (tags.length > 1) {
                                        // Process the first set of tags,
                                        // which are groupBy
                                        final String nonGroupingTags = tags[1];
                                        final String[] ngTags = nonGroupingTags.substring(1, nonGroupingTags.length())
                                                .split(",");
                                        for (final String tag : ngTags) {
                                            final String[] tParts = tag.split("=");
                                            sub.addTag(tParts[0], tParts[1]);
                                        }

                                    }
                                }

                            }
                            query.addQuery(sub);
                        });
        }
        if (decoder.parameters().containsKey("tsuid")) {
            decoder.parameters().get("tsuid").forEach(ts -> {
                final SubQuery sub = new SubQuery();
                final int colon = ts.indexOf(':');
                if (-1 != colon) {
                    sub.setAggregator(ts.substring(0, colon));
                }
                for (final String tsuid : ts.substring(colon + 1).split(",")) {
                    sub.addTsuid(tsuid);
                }
                query.addQuery(sub);
            });
        }
        return query;
    }

    public void setHttpRequest(FullHttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    public FullHttpRequest getHttpRequest() {
        return httpRequest;
    }
}
