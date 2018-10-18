package timely.store;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.adapter.accumulo.MetricAdapter;

public class MetricAgeOffIterator extends WrappingIterator implements OptionDescriber {

    private static final Logger LOG = LoggerFactory.getLogger(MetricAgeOffIterator.class);
    public static final String AGE_OFF_PREFIX = "ageoff.";
    public static final String DEFAULT_AGEOFF_KEY = "default";

    /* set in init */
    private PatriciaTrie<Long> ageoffs = null;
    private Long currentTime = 0L;
    private Long defaultAgeOff = 0L;
    private Long minAgeOff = Long.MAX_VALUE;
    private Long maxAgeOff = Long.MIN_VALUE;
    /* changes at runtime */
    private final BytesWritable prevMetricBytes = new BytesWritable();
    private Long prevAgeOff = null;
    private Range range;
    private Collection<ByteSequence> columnFamilies;
    private boolean inclusive;

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        MetricAgeOffIterator iter = new MetricAgeOffIterator();
        iter.ageoffs = this.ageoffs;
        iter.defaultAgeOff = this.defaultAgeOff;
        iter.currentTime = this.currentTime;
        iter.minAgeOff = this.minAgeOff;
        iter.maxAgeOff = this.maxAgeOff;
        return iter;
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
                LOG.trace("Adding {} to Trie with value {}", name, Long.parseLong(v));
                long ageoff = Long.parseLong(v);
                this.minAgeOff = Math.min(this.minAgeOff, ageoff);
                this.maxAgeOff = Math.max(this.maxAgeOff, ageoff);
                ageoffs.put(name, ageoff);
            }
        });
        defaultAgeOff = ageoffs.get(DEFAULT_AGEOFF_KEY);
        currentTime = System.currentTimeMillis();
    }

    private void seekIfNecessary() {
        if (super.hasTop()) {
            Key top = super.getTopKey();
            // If less than any configured ageoff, then we want to return this
            // K,V
            if ((this.currentTime - top.getTimestamp()) < this.minAgeOff) {
                return;
            }
            // If greater than any configured ageoff, then drop it
            if ((this.currentTime - top.getTimestamp()) > this.maxAgeOff) {
                LOG.trace("Current key is older than max age off, seeking to start of valid data");
                String metricName = MetricAdapter.decodeRowKey(top).getFirst();
                handleNewMetricName(metricName);
                seekPastAgedOffMetricData(metricName, this.maxAgeOff);
                return;
            }
            if (isNextMetricTheSame(top.getRow())) {
                // this metric name is the same as previous
                if ((currentTime - top.getTimestamp()) > prevAgeOff) {
                    // We are in the same metric, seek to data we want to
                    // process
                    String metricName = new String(prevMetricBytes.copyBytes(), UTF_8);
                    LOG.trace("Current metric is older than age off for metric {}, seeking to start of valid data",
                            metricName);
                    seekPastAgedOffMetricData(metricName, prevAgeOff);
                    return;
                }
            } else {
                // Metric name is different or prev information is not set
                String metricName = MetricAdapter.decodeRowKey(top).getFirst();
                handleNewMetricName(metricName);
                if (currentTime - top.getTimestamp() > prevAgeOff) {
                    LOG.trace("New metric found, but older than age off for metric {}, seeking to start of valid data",
                            metricName);
                    seekPastAgedOffMetricData(metricName, prevAgeOff);
                }
            }
        }
    }

    private boolean isNextMetricTheSame(Text nextRow) {
        byte[] next = nextRow.getBytes();
        if (next.length > prevMetricBytes.getLength()
                && 0 == prevMetricBytes.compareTo(next, 0, prevMetricBytes.getLength())
                && nextRow.charAt(prevMetricBytes.getLength()) == 0x00) {
            return true;
        } else {
            return false;
        }
    }

    private void handleNewMetricName(String metricName) {
        byte[] b = metricName.getBytes(UTF_8);
        prevMetricBytes.set(b, 0, b.length);
        prevAgeOff = ageoffs.get(metricName);
        if (null == prevAgeOff) {
            // no specific ageoff for this metric name, use default
            prevAgeOff = defaultAgeOff;
        }
    }

    private void seekPastAgedOffMetricData(String metricName, long ageOffTime) {
        long timeTarget = currentTime - ageOffTime;
        Range newRange;
        if (null == this.range) {
            byte[] newStartRow = MetricAdapter.encodeRowKey(metricName, timeTarget);
            newRange = new Range(new Key(new Text(newStartRow), timeTarget), null);
        } else {
            Key startKey = new Key(new Text(MetricAdapter.encodeRowKey(metricName, timeTarget)), timeTarget);
            Pair<String, Long> start = MetricAdapter.decodeRowKey(startKey);
            byte[] newStartRow = MetricAdapter.encodeRowKey(start.getFirst(), timeTarget);
            // @formatter:off
            Key newStartKey = new Key(newStartRow, 
            		null != startKey.getColumnFamily() ? startKey.getColumnFamilyData().getBackingArray() : null, 
            		null != startKey.getColumnQualifier() ? startKey.getColumnQualifierData().getBackingArray() : null,
                    null != startKey.getColumnVisibility() ? startKey.getColumnVisibility().getBytes() : null,
                    timeTarget, startKey.isDeleted());
            // @formatter:on
            if (this.range.getEndKey() == null || this.range.getEndKey().compareTo(newStartKey) >= 0) {
                newRange = new Range(newStartKey, true, this.range.getEndKey(), this.range.isEndKeyInclusive());
            } else {
                newRange = new Range(this.range.getEndKey(), false,
                        this.range.getEndKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), false);
            }
        }
        try {
            LOG.trace("Seeking to: {}", newRange);
            this.seek(newRange, columnFamilies, inclusive);
        } catch (IOException e) {
            LOG.error("Error seeking to new range: " + newRange, e);
        }

    }

    @Override
    public void next() throws IOException {
        super.next();
        seekIfNecessary();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.range = range;
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;
        super.seek(range, columnFamilies, inclusive);
        seekIfNecessary();
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions("metric-age-off", "Iterator that ages off time series data for Timely metrics",
                Collections.singletonMap(AGE_OFF_PREFIX + DEFAULT_AGEOFF_KEY, "default age off days"),
                Collections.singletonList("Additional metric age off properties where the metric is specified as "
                        + AGE_OFF_PREFIX
                        + " the metric name and the value is an integer representing the number of days to keep"));
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        if (null == options.get(AGE_OFF_PREFIX + DEFAULT_AGEOFF_KEY)) {
            throw new IllegalArgumentException(DEFAULT_AGEOFF_KEY + " must be configured for MetricAgeOffFilter");
        }
        return true;
    }

}
