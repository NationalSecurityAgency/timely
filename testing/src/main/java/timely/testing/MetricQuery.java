package timely.testing;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import timely.api.request.timeseries.QueryRequest;
import timely.util.JsonUtil;

import java.util.Map;
import java.util.Optional;

public class MetricQuery {

    private QueryRequest queryRequest = new QueryRequest();
    private QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();

    public MetricQuery(String metric, long begin, long end) {
        this.queryRequest.addQuery(subQuery);
        this.queryRequest.setStart(begin);
        this.queryRequest.setEnd(end);
        this.subQuery.setMetric(metric);
        this.subQuery.setDownsample(Optional.of("1ms-avg"));
    }

    public MetricQuery(String metric, long begin, long end, Map<String, String> tags) {
        this.queryRequest.addQuery(subQuery);
        this.queryRequest.setStart(begin);
        this.queryRequest.setEnd(end);
        this.subQuery.setMetric(metric);
        this.subQuery.setDownsample(Optional.of("1ms-avg"));
        this.subQuery.setTags(tags);
    }

    public void setBegin(long begin) {
        this.queryRequest.setStart(begin);
    }

    public void setEnd(long end) {
        this.queryRequest.setEnd(end);
    }

    public void setMetric(String metric) {
        this.subQuery.setMetric(metric);
    }

    public void setDownsample(String downsample) {
        this.subQuery.setDownsample(Optional.of(downsample));
    }

    public void setAggregator(String aggregator) {
        this.subQuery.setAggregator(aggregator);
    }

    public void setIsRate(boolean isRate) {
        this.subQuery.setRate(isRate);
    }

    public void setCounterOptions(long max, long resetValue) {
        QueryRequest.RateOption rateOption = new QueryRequest.RateOption();
        rateOption.setCounter(true);
        rateOption.setCounterMax(max);
        rateOption.setResetValue(resetValue);
        this.subQuery.setRateOptions(rateOption);
    }

    public void setTags(Map<String, String> tags) {
        this.subQuery.setTags(tags);
    }

    public String getJson() {

        String query = null;
        try {
            query = JsonUtil.getObjectMapper().writeValueAsString(queryRequest);

        } catch (Exception e) {

        }
        return query;
    }
}

