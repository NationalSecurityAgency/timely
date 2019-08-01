package timely.store.compaction.util;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.AggregatingFormatter;

public class TabletMetadataFormatter extends AggregatingFormatter {

    private final TabletMetadataView view;

    public TabletMetadataFormatter() {
        this(new TabletMetadataView());
    }

    public TabletMetadataFormatter(TabletMetadataView view) {
        this.view = view;
    }

    @Override
    public void aggregateStats(Map.Entry<Key, Value> entry) {
        view.addEntry(entry);
    }

    @Override
    protected String getStats() {
        return view.toText(TimeUnit.DAYS);
    }
}
