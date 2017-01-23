package timely.sample;

import timely.sample.aggregators.Avg;
import timely.sample.aggregators.Count;
import timely.sample.aggregators.Dev;
import timely.sample.aggregators.Max;
import timely.sample.aggregators.Min;
import timely.sample.aggregators.Sum;

public interface Aggregator {

    String NONE = "none";

    /**
     * Given the current value, and the current count, incorporate the new value
     * into current and return the result.
     * 
     * @param current
     *            last returned value from aggregate, or zero
     * @param count
     *            the number of items aggregated previously into current
     * @param update
     *            a new value to incorporate
     * @return the combination of update and current
     */
    double aggregate(double current, int count, double update);

    /**
     * Compute the final value given the aggregated amount and the number of
     * items in the aggregate.
     * 
     * @param current
     *            the last value returned from aggregate
     * @param count
     *            the number of values in the aggregate
     * @return the combination of the current value given the count
     */
    double last(double current, int count);

    static Class<? extends Aggregator> getAggregator(String aggregatorName) {
        if (aggregatorName == null) {
            return null;
        }
        switch (aggregatorName) {
            case "sum":
                return Sum.class;
            case "max":
                return Max.class;
            case "dev":
                return Dev.class;
            case "min":
                return Min.class;
            case "avg":
                return Avg.class;
            case "count":
                return Count.class;
            default:
                return null;
        }
    }
}
