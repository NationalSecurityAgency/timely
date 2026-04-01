package timely.collectd.plugin;

public interface WritesMetrics {

    void write(MetricData metricData);
}
