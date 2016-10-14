package timely.store.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.adapter.accumulo.MetricAdapter;

public class RateIterator extends SlidingWindowIterator {

    public static final Logger LOG = LoggerFactory.getLogger(RateIterator.class);

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        Map<String, String> opts = new HashMap<>(options);
        opts.put(SlidingWindowIterator.FILTER, "-1,1");
        super.init(source, opts, env);
    }

    @Override
    protected void compute() {

        Long firstTs = window.get(0).getFirst().getTimestamp();
        Double firstVal = MetricAdapter.decodeValue(window.get(0).getSecond().get()) * -1;
        LOG.debug("first ts:{}, value:{}", firstTs, firstVal);

        Long secondTs = window.get(1).getFirst().getTimestamp();
        Double secondVal = MetricAdapter.decodeValue(window.get(1).getSecond().get());
        LOG.debug("second ts:{}, value:{}", secondTs, secondVal);

        Double result = ((firstVal + secondVal) / (secondTs - firstTs));
        LOG.trace("compute - result: {}", result);
        topKey = window.getLast().getFirst();
        topValue = new Value(MetricAdapter.encodeValue(result));
    }

}
