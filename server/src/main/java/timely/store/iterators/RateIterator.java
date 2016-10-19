package timely.store.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RateIterator extends TimeSeriesGroupingIterator {

    public static final Logger LOG = LoggerFactory.getLogger(RateIterator.class);

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        Map<String, String> opts = new HashMap<>(options);
        opts.put(TimeSeriesGroupingIterator.FILTER, "-1,1");
        super.init(source, opts, env);
    }

    @Override
    protected Double compute(Map<Key, Double> values) {

        Iterator<Entry<Key, Double>> iter = values.entrySet().iterator();
        Entry<Key, Double> first = iter.next();
        Long firstTs = first.getKey().getTimestamp();
        Double firstVal = first.getValue() * -1;
        LOG.debug("first ts:{}, value:{}", firstTs, firstVal);

        Entry<Key, Double> second = iter.next();
        Long secondTs = second.getKey().getTimestamp();
        Double secondVal = second.getValue();
        LOG.debug("second ts:{}, value:{}", secondTs, secondVal);

        Double result = ((firstVal + secondVal) / (secondTs - firstTs));
        LOG.trace("compute - result: {}", result);
        return result;
    }

}
