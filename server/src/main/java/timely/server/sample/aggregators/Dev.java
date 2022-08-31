package timely.server.sample.aggregators;

import timely.server.sample.Aggregator;

public class Dev implements Aggregator {

    @Override
    public double aggregate(double current, int count, double update) {
        return current + (update * update);
    }

    @Override
    public double last(double current, int count) {
        if (count == 1) {
            return Math.sqrt(current);
        }
        return Math.sqrt(current / (count - 1));
    }

}
