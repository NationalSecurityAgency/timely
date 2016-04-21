package timely.sample.aggregators;

import timely.sample.Aggregator;

public class Max implements Aggregator {

    @Override
    public double aggregate(double current, int count, double update) {
        if (count == 0) {
            return update;
        }
        return Math.max(current, update);
    }

    @Override
    public double last(double current, int count) {
        return current;
    }
}
