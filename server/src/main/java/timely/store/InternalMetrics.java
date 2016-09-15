package timely.store;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.model.Metric;
import timely.model.Tag;

import com.google.common.util.concurrent.AtomicDouble;

public class InternalMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(InternalMetrics.class);

    private static final String METRICS_RECEIVED_METRIC = "timely.metrics.received";
    private static final String META_KEYS_METRIC = "timely.keys.meta.inserted";
    private static final String METRIC_KEYS_METRIC = "timely.keys.metric.inserted";
    private static final String HOSTNAME_TAG = "host";

    private String hostName = "localhost";
    private AtomicDouble numMetricsReceived = new AtomicDouble(0);
    private AtomicDouble numMetaKeysInserted = new AtomicDouble(0);
    private AtomicDouble numMetricKeysInserted = new AtomicDouble(0);
    private List<Tag> tags = new ArrayList<Tag>();

    public InternalMetrics() {
        super();
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("Error getting hostname", e);
        }
        tags.add(new Tag(HOSTNAME_TAG, hostName));
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

    public List<Metric> getMetricsAndReset() {
        List<Metric> metrics = new ArrayList<Metric>();
        long ts = System.currentTimeMillis();
        metrics.add(new Metric(METRICS_RECEIVED_METRIC, ts, numMetricsReceived.get(), tags));
        numMetricsReceived.set(0);
        metrics.add(new Metric(META_KEYS_METRIC, ts, numMetaKeysInserted.get(), tags));
        numMetaKeysInserted.set(0);
        metrics.add(new Metric(METRIC_KEYS_METRIC, ts, numMetricKeysInserted.get(), tags));
        numMetricKeysInserted.set(0);
        return metrics;
    }

}
