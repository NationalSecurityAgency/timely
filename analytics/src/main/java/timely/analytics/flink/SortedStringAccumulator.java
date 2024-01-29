package timely.analytics.flink;

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * Custom accumulator that retains a sorted map of strings and their counts
 *
 */
public class SortedStringAccumulator implements Accumulator<String,ConcurrentSkipListMap<String,MutableLong>> {

    private static final long serialVersionUID = 1L;

    private ConcurrentSkipListMap<String,MutableLong> values = new ConcurrentSkipListMap<>();

    @Override
    public void add(String value) {
        if (values.containsKey(value)) {
            values.get(value).increment();
        } else {
            values.put(value, new MutableLong(1));
        }
    }

    @Override
    public ConcurrentSkipListMap<String,MutableLong> getLocalValue() {
        return values;
    }

    @Override
    public void resetLocal() {
        values.clear();
    }

    @Override
    public void merge(Accumulator<String,ConcurrentSkipListMap<String,MutableLong>> other) {
        other.getLocalValue().forEach((k, v) -> {
            if (values.containsKey(k)) {
                values.get(k).add(v.longValue());
            } else {
                values.put(k, v);
            }
        });
    }

    @Override
    public Accumulator<String,ConcurrentSkipListMap<String,MutableLong>> clone() {
        SortedStringAccumulator a = new SortedStringAccumulator();
        a.getLocalValue().putAll(this.values);
        return a;
    }

}
