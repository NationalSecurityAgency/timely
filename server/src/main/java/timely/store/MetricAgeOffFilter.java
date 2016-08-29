package timely.store;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
    private ByteBuffer buf = ByteBuffer.allocate(256);
    private byte[] prevMetricBytes = null;
    private Long prevAgeOff = null;

    @Override
    public boolean accept(Key k, Value v) {

        // Metrics table row format is PairLexicoder<>(new StringLexicoder(),
        // new LongLexicoder());
        // We want to get the bytes for the metric name from the row
        buf.clear();
        ByteSequence row = k.getRowData();
        for (int i = row.offset(); i < row.length(); i++) {
            byte b = row.byteAt(i);
            if (b == 0x00) {
                break;
            } else {
                buf.put(b);
            }
        }
        byte[] metricName = new byte[buf.position()];
        buf.position(0);
        buf.get(metricName);

        // Try to avoid String object creation here. If bytes are the same
        // as previous metric name, then just reference the String.
        Long ageoff = null;
        if (null != prevMetricBytes && null != prevAgeOff) {
            if (Arrays.equals(prevMetricBytes, metricName)) {
                LOG.trace("This metric name the same as previous");
                ageoff = prevAgeOff;
            }
        }
        // If no match, set accordingly
        if (null == ageoff) {
            LOG.trace("This metric name not the same as previous");
            prevMetricBytes = metricName;
            prevAgeOff = ageoffs.get(new String(prevMetricBytes, UTF_8));
            ageoff = prevAgeOff;
            if (null == ageoff) {
                // no specific ageoff for this metric name, use default
                ageoff = defaultAgeOff;
                prevAgeOff = ageoff;
            }
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
                ageoffs.put(k, Long.parseLong(v));
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
