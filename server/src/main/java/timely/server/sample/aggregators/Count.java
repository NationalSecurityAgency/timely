package timely.server.sample.aggregators;

import timely.server.sample.Aggregator;

public class Count implements Aggregator {

    @Override
    public double aggregate(double current, int count, double update) {
        return 0.;
    }

    @Override
    public double last(double current, int count) {
        return count;
    }

}
