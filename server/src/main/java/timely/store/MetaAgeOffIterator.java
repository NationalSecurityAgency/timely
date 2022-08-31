package timely.store;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.model.Meta;

public class MetaAgeOffIterator extends WrappingIterator implements OptionDescriber {

    private static final Logger log = LoggerFactory.getLogger(MetaAgeOffIterator.class);
    public static final String AGE_OFF_PREFIX = "ageoff.";
    public static final String DEFAULT_AGEOFF_KEY = "default";

    /* set in init */
    private PatriciaTrie<Long> ageoffs = null;
    private Long currentTime = 0L;
    private Long defaultAgeOff = 0L;
    private Long minAgeOff = Long.MAX_VALUE;
    private Long maxAgeOff = Long.MIN_VALUE;

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        MetaAgeOffIterator iter = new MetaAgeOffIterator();
        iter.ageoffs = this.ageoffs;
        iter.defaultAgeOff = this.defaultAgeOff;
        iter.currentTime = this.currentTime;
        iter.minAgeOff = this.minAgeOff;
        iter.maxAgeOff = this.maxAgeOff;
        return iter;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        validateOptions(options);
        ageoffs = new PatriciaTrie<>();
        options.forEach((k, v) -> {
            if (k.startsWith(AGE_OFF_PREFIX)) {
                String name = k.substring(AGE_OFF_PREFIX.length());
                log.trace("Adding {} to Trie with value {}", name, Long.parseLong(v));
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
    public void next() throws IOException {
        super.next();
        skipExpiredData();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);
        skipExpiredData();
    }

    private void skipExpiredData() throws IOException {

        boolean done = false;
        while (!done && super.hasTop()) {
            Key top = super.getTopKey();
            long dataAge = this.currentTime - top.getTimestamp();
            // If less than any configured ageoff, then we want to return this K,V
            if (dataAge < this.minAgeOff) {
                done = true;
            } else {
                String metricName = Meta.parse(top).getMetric();
                long metaAgeOff = ageoffs.getOrDefault(metricName, defaultAgeOff);
                if (dataAge > metaAgeOff) {
                    // data beyond ageOff, so skip
                    super.next();
                } else {
                    done = true;
                }
            }
        }
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions("meta-age-off", "Iterator that ages off meta entries for Timely metrics",
                        Collections.singletonMap(AGE_OFF_PREFIX + DEFAULT_AGEOFF_KEY, "default age off days"),
                        Collections.singletonList("Additional meta age off properties where the value is specified as " + AGE_OFF_PREFIX
                                        + " the metric name and the value is an integer representing the number of days to keep"));
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (null == options.get(AGE_OFF_PREFIX + DEFAULT_AGEOFF_KEY)) {
            throw new IllegalArgumentException(DEFAULT_AGEOFF_KEY + " must be configured for MetaAgeOffFilter");
        }
        return true;
    }

}
