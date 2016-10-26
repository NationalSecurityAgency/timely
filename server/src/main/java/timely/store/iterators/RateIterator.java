package timely.store.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
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
    protected Double compute(List<Pair<Key, Double>> values) {

        Iterator<Pair<Key, Double>> iter = values.iterator();
        Pair<Key, Double> first = iter.next();
        Long firstTs = first.getFirst().getTimestamp();
        Double firstVal = first.getSecond() * -1;
        LOG.trace("first ts:{}, value:{}", firstTs, firstVal);

        Pair<Key, Double> second = iter.next();
        Long secondTs = second.getFirst().getTimestamp();
        Double secondVal = second.getSecond();
        LOG.trace("second ts:{}, value:{}", secondTs, secondVal);

        long timeDiff = secondTs - firstTs;
        if (timeDiff == 0) {
            return 0.0D;
        }
        Double result = ((firstVal + secondVal) / timeDiff);
        LOG.trace("compute - result: {}", result);
        return result;
    }

}
