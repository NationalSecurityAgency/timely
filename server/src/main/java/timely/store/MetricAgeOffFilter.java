package timely.store;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;

public class MetricAgeOffFilter extends AgeOffFilter {

    public static final String METRIC = "metric.name";
    private String name = null;

    @Override
    public boolean accept(Key k, Value v) {
        String row = k.getRow().toString();
        // Don't decode the row, just check the leading portion
        if (row.startsWith(name)) {
            return super.accept(k, v);
        } else {
            return true;
        }
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);
        this.name = options.get(METRIC);
        if (null == name) {
            throw new IllegalArgumentException(METRIC + " must be configured for MetricAgeOffFilter");
        }
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        MetricAgeOffFilter filter = (MetricAgeOffFilter) super.deepCopy(env);
        filter.name = this.name;
        return filter;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.addNamedOption(METRIC, "metric name");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        if (null == options.get(METRIC)) {
            throw new IllegalArgumentException(METRIC + " must be configured for MetricAgeOffFilter");
        }
        return super.validateOptions(options);
    }

}
