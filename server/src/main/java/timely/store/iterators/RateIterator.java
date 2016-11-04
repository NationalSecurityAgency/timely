package timely.store.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.request.timeseries.QueryRequest;

public class RateIterator extends TimeSeriesGroupingIterator {

    public static final Logger LOG = LoggerFactory.getLogger(RateIterator.class);
    private static final String COUNTER_MAX = "rate.counter.max";
    private static final String RATE_RESET_VALUE = "rate.reset.value";

    private boolean isCounter = false;
    private long maxCounter = 0;
    private long resetValue = 0;

    public static void setRateOptions(IteratorSetting is, QueryRequest.RateOption options) {
        if (options != null && options.isCounter()) {
            LOG.trace("Setting rate counter options cm:{}, rv:{}", options.getCounterMax(), options.getResetValue());
            is.addOption(COUNTER_MAX, Long.toString(options.getCounterMax()));
            is.addOption(RATE_RESET_VALUE, Long.toString(options.getResetValue()));
        }
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        if (options.containsKey(COUNTER_MAX)) {
            this.isCounter = true;
            this.maxCounter = Long.parseLong(options.get(COUNTER_MAX));
            this.resetValue = Long.parseLong(options.get(RATE_RESET_VALUE));
            LOG.trace("Setting rate counter options cm:{}, rv:{}", this.maxCounter, this.resetValue);
        }
        Map<String, String> opts = new HashMap<>(options);
        opts.put(TimeSeriesGroupingIterator.FILTER, "-1,1");
        super.init(source, opts, env);
    }

    @Override
    protected Double compute(List<Pair<Key, Double>> values) {

        Iterator<Pair<Key, Double>> iter = values.iterator();
        Pair<Key, Double> first = iter.next();
        Long firstTs = first.getFirst().getTimestamp();
        Double firstVal = first.getSecond();
        LOG.trace("first ts:{}, value:{}", firstTs, firstVal);

        Pair<Key, Double> second = iter.next();
        Long secondTs = second.getFirst().getTimestamp();
        Double secondVal = second.getSecond();
        LOG.trace("second ts:{}, value:{}", secondTs, secondVal);

        if (isCounter && (secondVal < firstVal)) {
            if (maxCounter > 0) {
                secondVal += maxCounter;
                LOG.trace("second counter reset based on max:{} to ts:{}, value:{}", maxCounter, secondTs, secondVal);
            } else {
                secondVal += firstVal;
                LOG.trace("second counter reset based on first:{} to ts:{}, value:{}", firstVal, secondTs, secondVal);
            }
        }

        long timeDiff = secondTs - firstTs;
        if (timeDiff == 0) {
            return 0.0D;
        }
        Double result = ((secondVal - firstVal) / timeDiff);
        LOG.trace("compute - result: {}", result);

        if (isCounter && resetValue > 0 && result > resetValue) {
            result = 0.0D;
            LOG.trace("compute - reset result base on {} to {}", resetValue, result);
        }

        return result;
    }

}
