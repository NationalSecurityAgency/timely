package timely.analytics.flink;

import org.apache.flink.api.java.functions.KeySelector;

import timely.api.response.MetricResponse;

/**
 * Uniquely defines a time series in a Flink stream. A unique time series is the metric name and all of its tags, to include visibility if it exists.
 */
public class MetricKeySelector implements KeySelector<MetricResponse,String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String getKey(MetricResponse m) throws Exception {
        return m.getMetric() + m.getTags().toString();
    }

}
