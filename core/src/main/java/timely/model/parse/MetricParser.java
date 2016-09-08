package timely.model.parse;

import com.google.common.base.Splitter;
import timely.model.Metric;

import java.util.List;


public class MetricParser implements Parser<Metric> {

    private static final Splitter spaceSplitter = Splitter.on(" ").omitEmptyStrings().trimResults();

    @Override
    public Metric parse(String t) {

        Metric.Builder builder = Metric.Builder.newInstance();

        // put specification
        //
        // put <metricName> <timestamp> <value> <tagK=tagV> <tagK=tagV> ...
        List<String> parts = spaceSplitter.splitToList(t);
        parts.remove(0);  // put
        builder.name(parts.remove(0));
        builder.value(Long.parseLong(parts.remove(0)),
                Double.parseDouble(parts.remove(0)));
        parts.stream().forEach(rawTag -> {
                    builder.tag(rawTag);
                });
        return builder.build();
    }
}
