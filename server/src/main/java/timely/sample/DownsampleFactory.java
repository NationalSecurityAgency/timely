package timely.sample;

import io.netty.handler.codec.http.HttpResponseStatus;
import timely.api.response.TimelyException;

public class DownsampleFactory {

    private final long start;
    private final long end;
    private final long period;
    private final Class<? extends Aggregator> aggClass;

    public DownsampleFactory(long start, long end, long period, Class<? extends Aggregator> aggClass) {
        this.start = start;
        this.end = end;
        this.period = period;
        this.aggClass = aggClass;
    }

    public Downsample create() throws TimelyException {
        try {
            return new Downsample(start, end, period, aggClass.newInstance());
        } catch (InstantiationException | IllegalAccessException e) {
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    "Error creating aggregator class: " + aggClass, e.getMessage(), e);
        }
    }
}
