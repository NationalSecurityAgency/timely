package timely.server.sample.aggregators;

import timely.server.sample.Aggregator;

public class Avg implements Aggregator {

    @Override
    public double aggregate(double current, int count, double update) {
        return current + update;
    }

    @Override
    public double last(double current, int count) {
        return current / count;
    }

}
