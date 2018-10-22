package timely.store;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Configuration;
import timely.model.Metric;
import timely.model.Tag;

import com.google.common.util.concurrent.AtomicDouble;

public class InternalMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(InternalMetrics.class);

    private static final String METRICS_RECEIVED_METRIC = "timely.metrics.received";
    private static final String META_KEYS_METRIC = "timely.keys.meta.inserted";
    private static final String METRIC_KEYS_METRIC = "timely.keys.metric.inserted";
    private static final String QUERIES_COMPLETED = "timely.query.num.completed";
    private static final String QUERY_RETURN_TIME = "timely.query.return.time";
    private static final String METRICS_RETURNED = "timely.query.metrics.returned";
    private static final String METRIC_RETURN_RATE = "timely.query.metrics.returned.rate";

    private static final String HOSTNAME_TAG = "host";

    private String hostName = "localhost";
    private AtomicDouble numMetricsReceived = new AtomicDouble(0);
    private AtomicDouble numMetaKeysInserted = new AtomicDouble(0);
    private AtomicDouble numMetricKeysInserted = new AtomicDouble(0);
    private AtomicDouble numQueriesCompleted = new AtomicDouble(0);
    private AtomicDouble numMetricsReturned = new AtomicDouble(0);
    private AtomicDouble elapsedQueryTime = new AtomicDouble(0);

    private List<Tag> tags = new ArrayList<Tag>();

    public InternalMetrics(Configuration conf) {
        super();
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("Error getting hostname", e);
        }
        String instance = conf.getInstance();
        if (instance == null) {
            tags.add(new Tag(HOSTNAME_TAG, hostName));
        } else {
            tags.add(new Tag(HOSTNAME_TAG, hostName + "_" + instance));
        }
    }

    public void incrementMetricsReceived(long num) {
        numMetricsReceived.addAndGet(num);
    }

    public void incrementMetaKeysInserted(long num) {
        numMetaKeysInserted.addAndGet(num);
    }

    public void incrementMetricKeysInserted(long num) {
        numMetricKeysInserted.addAndGet(num);
    }

    public void addQueryResponse(long numMetrics, long elapsedTime) {
        numQueriesCompleted.addAndGet(1);
        numMetricsReturned.addAndGet(numMetrics);
        elapsedQueryTime.addAndGet(elapsedTime);
    }

    public List<Metric> getMetricsAndReset() {
        List<Metric> metrics = new ArrayList<Metric>();
        long ts = System.currentTimeMillis();
        metrics.add(new Metric(METRICS_RECEIVED_METRIC, ts, numMetricsReceived.get(), tags));
        numMetricsReceived.set(0);
        metrics.add(new Metric(META_KEYS_METRIC, ts, numMetaKeysInserted.get(), tags));
        numMetaKeysInserted.set(0);
        metrics.add(new Metric(METRIC_KEYS_METRIC, ts, numMetricKeysInserted.get(), tags));
        numMetricKeysInserted.set(0);

        metrics.add(new Metric(QUERIES_COMPLETED, ts, numQueriesCompleted.get(), tags));
        metrics.add(new Metric(METRICS_RETURNED, ts, numMetricsReturned.get(), tags));

        if (elapsedQueryTime.get() > 0) {
            // average response time per query
            double averageQueryTime = (numQueriesCompleted.get() / elapsedQueryTime.get());
            metrics.add(new Metric(QUERY_RETURN_TIME, ts, averageQueryTime, tags));

            // metrics returned per minute
            double rate = (numMetricsReturned.get() / (elapsedQueryTime.get() / (1000 * 60)));
            metrics.add(new Metric(METRIC_RETURN_RATE, ts, rate, tags));
        } else {
            // if no elapsed time, then rate is zero
            metrics.add(new Metric(METRIC_RETURN_RATE, ts, 0, tags));
        }
        numQueriesCompleted.set(0);
        numMetricsReturned.set(0);
        elapsedQueryTime.set(0);
        return metrics;
    }

}
