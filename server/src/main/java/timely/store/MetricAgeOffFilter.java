package timely.store;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricAgeOffFilter extends Filter {

    private static final Logger LOG = LoggerFactory.getLogger(MetricAgeOffFilter.class);
    public static final String AGE_OFF_PREFIX = "ageoff.";
    public static final String DEFAULT_AGEOFF_KEY = "default";
    private PatriciaTrie<Long> ageoffs = null;
    private Long currentTime = 0L;
    private Long defaultAgeOff = 0L;
    private Long minAgeOff = Long.MAX_VALUE;
    private Long maxAgeOff = Long.MIN_VALUE;
    private byte[] prevMetricBytes = null;
    private Long prevAgeOff = null;

    @Override
    public boolean accept(Key k, Value v) {

        // If less than any configured ageoff, then keep it
        if ((this.currentTime - k.getTimestamp()) < this.minAgeOff) {
            return true;
        }
        // If greater than any configured ageoff, then drop it
        if ((this.currentTime - k.getTimestamp()) > this.maxAgeOff) {
            return false;
        }
        // There is a high probability that this key will have
        // the same metric name as the last key.
        ByteSequence rowData = k.getRowData();
        int rowStart = rowData.offset();
        int i = 0;
        if (null != prevMetricBytes && (rowData.length() >= (rowStart + prevMetricBytes.length + 1))
                && (rowData.byteAt(rowStart + prevMetricBytes.length + 1) == 0x00)) {
            // Double check metric name is the same
            boolean same = true;
            for (; i < prevMetricBytes.length; i++) {
                if (prevMetricBytes[i] != rowData.byteAt(rowStart + i)) {
                    same = false;
                    break;
                }
            }
            if (same) {
                if ((currentTime - k.getTimestamp()) > prevAgeOff)
                    return false;
                return true;
            }
        }

        // Metric name is different or prev information is not set

        // We have not found the null byte up to this point.
        // Keep scanning for the null byte
        int y = rowStart + i;
        for (; y < rowData.length(); y++) {
            byte b = rowData.byteAt(y);
            if (b == 0x00) {
                break;
            }
        }
        byte[] metricName = new byte[(y - rowStart)];
        System.arraycopy(rowData.getBackingArray(), rowStart, metricName, 0, (y - rowStart));

        prevMetricBytes = metricName;
        prevAgeOff = ageoffs.get(new String(prevMetricBytes, UTF_8));

        if (null == prevAgeOff) {
            // no specific ageoff for this metric name, use default
            prevAgeOff = defaultAgeOff;
        }
        if (currentTime - k.getTimestamp() > prevAgeOff)
            return false;
        return true;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);
        validateOptions(options);
        ageoffs = new PatriciaTrie<>();
        options.forEach((k, v) -> {
            if (k.startsWith(AGE_OFF_PREFIX)) {
                String name = k.substring(AGE_OFF_PREFIX.length());
                LOG.trace("Adding {} to Trie with value", name, Long.parseLong(v));
                long ageoff = Long.parseLong(v);
                this.minAgeOff = Math.min(this.minAgeOff, ageoff);
                this.maxAgeOff = Math.max(this.maxAgeOff, ageoff);
                ageoffs.put(name, ageoff);
            }
        });
        defaultAgeOff = ageoffs.get(DEFAULT_AGEOFF_KEY);
        currentTime = System.currentTimeMillis();
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        MetricAgeOffFilter filter = (MetricAgeOffFilter) super.deepCopy(env);
        filter.ageoffs = this.ageoffs;
        return filter;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.addNamedOption(DEFAULT_AGEOFF_KEY, "default age off days");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        if (null == options.get(MetricAgeOffFilter.AGE_OFF_PREFIX + DEFAULT_AGEOFF_KEY)) {
            throw new IllegalArgumentException(DEFAULT_AGEOFF_KEY + " must be configured for MetricAgeOffFilter");
        }
        return super.validateOptions(options);
    }

}
