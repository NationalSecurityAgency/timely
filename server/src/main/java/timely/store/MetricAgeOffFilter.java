package timely.store;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.ByteBuffer;
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
    public static final String DEFAULT_AGEOFF_KEY = "default";
    private PatriciaTrie<Long> ageoffs = null;
    private Long currentTime = 0L;
    private Long defaultAgeOff = 0L;
    private Long minAgeOff = Long.MAX_VALUE;
    private Long maxAgeOff = Long.MIN_VALUE;
    private ByteBuffer buf = ByteBuffer.allocate(256);
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
        int i = Integer.MIN_VALUE;
        if (null != prevMetricBytes && null != prevAgeOff
                && (rowData.byteAt(rowStart + prevMetricBytes.length + 1) == 0x00)) {
            // Double check metric name is the same
            boolean same = true;
            for (i = 0; i < prevMetricBytes.length; i++) {
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

        // If i is not MIN_VALUE or zero, then we already know that we have
        // not found the null byte up to that point. We can copy those bytes
        // into the ByteBuffer and then start looking for the null byte
        // from that point.
        buf.clear();
        if (i != Integer.MIN_VALUE && i != 0) {
            for (int x = rowStart; x < i; x++) {
                buf.put(rowData.byteAt(x));
            }
        }
        if (i < 0) {
            i = 0;
        }
        // Keep scanning for the null byte
        for (int y = (rowStart + i); y < rowData.length(); y++) {
            byte b = rowData.byteAt(y);
            if (b == 0x00) {
                break;
            } else {
                buf.put(b);
            }
        }
        byte[] metricName = new byte[buf.position()];
        buf.position(0);
        buf.get(metricName);

        prevMetricBytes = metricName;
        prevAgeOff = ageoffs.get(new String(prevMetricBytes, UTF_8));
        Long ageoff = prevAgeOff;
        if (null == ageoff) {
            // no specific ageoff for this metric name, use default
            ageoff = defaultAgeOff;
            prevAgeOff = ageoff;
        }
        if (currentTime - k.getTimestamp() > ageoff)
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
            if (!k.equals(NEGATE)) {
                LOG.trace("Adding {} to Trie with value", k, Long.parseLong(v));
                long ageoff = Long.parseLong(v);
                this.minAgeOff = Math.min(this.minAgeOff, ageoff);
                this.maxAgeOff = Math.max(this.maxAgeOff, ageoff);
                ageoffs.put(k, ageoff);
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
        if (null == options.get(DEFAULT_AGEOFF_KEY)) {
            throw new IllegalArgumentException(DEFAULT_AGEOFF_KEY + " must be configured for MetricAgeOffFilter");
        }
        return super.validateOptions(options);
    }

}
