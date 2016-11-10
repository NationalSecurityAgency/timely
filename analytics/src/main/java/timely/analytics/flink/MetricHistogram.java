package timely.analytics.flink;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.streaming.util.serialization.SerializationSchema;

import timely.model.Tag;

public class MetricHistogram implements Serializable, SerializationSchema<MetricHistogram> {

    private static final long serialVersionUID = 1L;
    private static final String SPACE = " ";
    private static final String AVG = " sample=avg\n";
    private static final String COUNT = " sample=count\n";
    private static final String MAX = " sample=max\n";
    private static final String MIN = " sample=min\n";
    private static final String SUM = " sample=sum\n";
    private static final String p50 = " sample=50p\n";
    private static final String p75 = " sample=75p\n";
    private static final String p90 = " sample=90p\n";
    private static final String p99 = " sample=99p\n";

    private String metric = null;
    private List<Tag> tags = null;
    private long timestamp = 0L;
    private boolean initialized = false;
    private List<Double> values = new ArrayList<>();

    public void initialize(String metric, List<Tag> tags) {
        this.metric = metric;
        this.tags = tags;
        initialized = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public String getMetric() {
        return metric;
    }

    public List<Tag> getTags() {
        return tags;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void update(Double d, long timestamp) {
        values.add(d);
        this.timestamp = timestamp;
    }

    public void done() {
        Collections.sort(values);
    }

    public Double avg() {
        if (values.size() == 0) {
            return 0.0D;
        }
        return sum() / count();
    }

    public Double min() {
        if (values.size() == 0) {
            return 0.0D;
        }
        return values.get(0);
    }

    public Double max() {
        if (values.size() == 0) {
            return 0.0D;
        }
        return values.get(values.size() - 1);
    }

    public Double getPercentile(int p) {
        if (values.size() == 0) {
            return 0.0D;
        }
        int position = Math.round((p * 1.0f / 100) * values.size());
        position = (0 == position) ? 0 : position - 1;
        return values.get(position);
    }

    public Double count() {
        return values.size() * 1.0D;
    }

    public Double sum() {
        double sum = 0.0D;
        for (Double d : values) {
            sum += d;
        }
        return sum;
    }

    @Override
    public String toString() {
        done();
        StringBuilder buf = new StringBuilder();
        buf.append("metric: ").append(metric);
        buf.append(" tags: ").append(tags);
        buf.append(" timestamp: ").append(timestamp);
        buf.append(" count: ").append(count());
        buf.append(" min: ").append(min());
        buf.append(" max: ").append(max());
        buf.append(" sum: ").append(sum());
        buf.append(" 50p: ").append(getPercentile(50));
        buf.append(" 75p: ").append(getPercentile(75));
        buf.append(" 99p: ").append(getPercentile(99));
        return buf.toString();
    }

    public byte[] serialize(MetricHistogram histo) {
        StringBuilder buf = new StringBuilder();

        String tags = histo.getTags().stream().map(t -> t.join()).collect(Collectors.joining(" "));

        // @formatter:off
        buf.append("put ").append(histo.getMetric() + "_summarized")
            .append(SPACE).append(histo.getTimestamp())
            .append(SPACE).append(histo.min())
            .append(SPACE).append(tags).append(MIN);
        buf.append("put ").append(histo.getMetric() + "_summarized")
            .append(SPACE).append(histo.getTimestamp())
            .append(SPACE).append(histo.max())
            .append(SPACE).append(tags).append(MAX);
        buf.append("put ").append(histo.getMetric() + "_summarized")
            .append(SPACE).append(histo.getTimestamp())
            .append(SPACE).append(histo.count())
            .append(SPACE).append(tags).append(COUNT);
        buf.append("put ").append(histo.getMetric() + "_summarized")
            .append(SPACE).append(histo.getTimestamp())
            .append(SPACE).append(histo.sum())
            .append(SPACE).append(tags).append(SUM);
        buf.append("put ").append(histo.getMetric() + "_summarized")
            .append(SPACE).append(histo.getTimestamp())
            .append(SPACE).append(histo.avg())
            .append(SPACE).append(tags).append(AVG);
        buf.append("put ").append(histo.getMetric() + "_summarized")
            .append(SPACE).append(histo.getTimestamp())
            .append(SPACE).append(histo.getPercentile(50))
            .append(SPACE).append(tags).append(p50);
        buf.append("put ").append(histo.getMetric() + "_summarized")
            .append(SPACE).append(histo.getTimestamp())
            .append(SPACE).append(histo.getPercentile(75))
            .append(SPACE).append(tags).append(p75);
        buf.append("put ").append(histo.getMetric() + "_summarized")
            .append(SPACE).append(histo.getTimestamp())
            .append(SPACE).append(histo.getPercentile(90))
            .append(SPACE).append(tags).append(p90);
        buf.append("put ").append(histo.getMetric() + "_summarized")
            .append(SPACE).append(histo.getTimestamp())
            .append(SPACE).append(histo.getPercentile(99))
            .append(SPACE).append(tags).append(p99);
        // @formatter:off
        return buf.toString().getBytes();
    }

}
