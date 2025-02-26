package timely.server.sample.iterators;

import static org.apache.accumulo.core.conf.ConfigurationTypeHelper.getTimeInMillis;

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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.request.timeseries.QueryRequest;

public class RateIterator extends TimeSeriesGroupingIterator {

    private static final Logger log = LoggerFactory.getLogger(RateIterator.class);
    public static final String COUNTER_MAX = "rate.counter.max";
    public static final String RATE_RESET_VALUE = "rate.reset.value";
    public static final String RATE_INTERVAL = "rate.interval";

    private boolean isCounter = false;
    private long maxCounter = 0;
    private long resetValue = 0;
    private long interval = 1000;

    public static void setRateOptions(IteratorSetting is, QueryRequest.RateOption options) {
        if (options != null && options.isCounter()) {
            log.trace("Setting rate counter options cm:{}, rv:{}", options.getCounterMax(), options.getResetValue());
            is.addOption(COUNTER_MAX, Long.toString(options.getCounterMax()));
            is.addOption(RATE_RESET_VALUE, Long.toString(options.getResetValue()));
        }
        if (options != null && StringUtils.isNotBlank(options.getInterval())) {
            is.addOption(RATE_INTERVAL, Long.toString(getTimeInMillis(options.getInterval())));
        } else {
            is.addOption(RATE_INTERVAL, "1000");
        }
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (options.containsKey(COUNTER_MAX)) {
            this.isCounter = true;
            this.maxCounter = Long.parseLong(options.get(COUNTER_MAX));
            this.resetValue = Long.parseLong(options.get(RATE_RESET_VALUE));
        }
        if (options.containsKey(RATE_INTERVAL)) {
            this.interval = Long.parseLong(options.get(RATE_INTERVAL));
        }
        log.trace("Setting rate options interval:{} isCounter:{} cm:{}, rv:{}", this.interval, this.isCounter, this.maxCounter, this.resetValue);
        Map<String,String> opts = new HashMap<>(options);
        opts.put(FILTER, "-1,1");
        super.init(source, opts, env);
    }

    @Override
    protected Double compute(List<Pair<Key,Double>> values) {

        Iterator<Pair<Key,Double>> iter = values.iterator();
        Pair<Key,Double> first = iter.next();
        Long firstTs = first.getFirst().getTimestamp();
        Double firstVal = first.getSecond();
        log.trace("first ts:{}, value:{}", firstTs, firstVal);

        Pair<Key,Double> second = iter.next();
        Long secondTs = second.getFirst().getTimestamp();
        Double secondVal = second.getSecond();
        log.trace("second ts:{}, value:{}", secondTs, secondVal);

        if (isCounter && (secondVal < firstVal)) {
            if (maxCounter > 0) {
                // assume that the data has reset so add maxCounter to the lower value to calculate the difference
                secondVal += maxCounter;
                log.trace("second counter reset based on max:{} to ts:{}, value:{}", maxCounter, secondTs, secondVal);
            } else {
                // assume that the data has reset but no maxCounter set, so add the higher value
                // (approximate maxCounter) to the lower value to calculate the difference
                secondVal += firstVal;
                log.trace("second counter reset based on first:{} to ts:{}, value:{}", firstVal, secondTs, secondVal);
            }
        }

        long timeDiff = secondTs - firstTs;
        if (timeDiff == 0) {
            return 0.0D;
        }
        // (secondVal - firstVal) / timeDiff will be value per millisecond, so multiply by interval to get
        // value per interval. Example 60000 milliseconds in 1m, so multiply by 60000 milliseconds
        Double result = ((secondVal - firstVal) / timeDiff * interval);
        log.trace("compute - result: {}", result);

        if (isCounter && resetValue > 0 && result > resetValue) {
            result = 0.0D;
            log.trace("compute - reset result base on {} to {}", resetValue, result);
        }

        return result;
    }

}
