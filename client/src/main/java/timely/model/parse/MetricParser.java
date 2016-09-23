package timely.model.parse;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import timely.model.Metric;

import java.util.List;

public class MetricParser implements Parser<Metric> {

    private static final Splitter spaceSplitter = Splitter.on(" ").omitEmptyStrings().trimResults();

    @Override
    public Metric parse(String line) {

        // put specification
        //
        // put <metricName> <timestamp> <value> <tagK=tagV> <tagK=tagV> ...

        List<String> parts = spaceSplitter.splitToList(line);
        Preconditions.checkPositionIndex(3, parts.size());

        Metric.Builder builder = Metric.newBuilder();
        // index 0 is put
        builder.name(parts.get(1)); // metric name
        builder.value(Long.parseLong(parts.get(2)), Double.parseDouble(parts.get(3)));
        parts.stream().skip(4).forEach(builder::tag);
        return builder.build();
    }
}
