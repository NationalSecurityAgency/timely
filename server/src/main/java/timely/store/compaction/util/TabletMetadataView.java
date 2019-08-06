package timely.store.compaction.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.hadoop.io.Text;

public class TabletMetadataView {

    private final MetadataAccumulator accumulator;

    private static final int DEFAULT_TOP_COUNT = 25;

    public TabletMetadataView() {
        accumulator = new MetadataAccumulator();
    }

    public void addEntry(Collection<Map.Entry<Key, Value>> entries) {
        for (Map.Entry<Key, Value> entry : entries) {
            addEntry(entry);
        }
    }

    public void addEntry(Map.Entry<Key, Value> entry) {
        KeyExtent ke = new KeyExtent(entry.getKey().getRow(), (Text) null);
        if (ke.getEndRow() == null) {
            return;
        }

        Text cf = entry.getKey().getColumnFamily();
        Text cq = entry.getKey().getColumnQualifier();
        MetadataAccumulator.Entry candidate = accumulator.checkEntryState(ke.getEndRow());

        if (cf.equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
            String val = entry.getValue().toString();
            String[] valSplit = val.split(",");
            if (valSplit.length == 2) {
                long size = Long.parseLong(valSplit[0]);
                candidate.addFile(size);
            }
        } else if (cf.equals(MetadataSchema.TabletsSection.ServerColumnFamily.NAME)) {
            if (cq.equals(MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.getColumnQualifier())) {
                String timeVal = entry.getValue().toString();
                if (timeVal.startsWith("M")) {
                    long ms = Long.parseLong(timeVal.substring(1));
                    candidate.setMilliseconds(ms);
                }
            }
        } else if (cf.equals(MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.getColumnFamily())
                && cq.equals(MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.getColumnQualifier())) {
            ke = new KeyExtent(entry.getKey().getRow(), entry.getValue());
            candidate.setTablePrev(ke.getPrevEndRow());
        }
    }

    public Collection<MetadataAccumulator.Entry> getEntries() {
        accumulator.ensureState();
        return accumulator.getEntries();
    }

    public TabletSummary.Builder computeSummary() {
        return TabletSummary.newBuilder(getEntries());
    }

    public String toText() {
        return toText(TimeUnit.MILLISECONDS);
    }

    public String toText(TimeUnit unit) {
        if (getEntries().isEmpty()) {
            return "No results";
        }

        StringBuilder sb = new StringBuilder();
        TabletSummary summary = computeSummary().build();
        Map<TabletStatisticType, StatisticalSummary> tabletAggregate = summary.aggregateSummary();
        StatisticalSummary tabletSizes = tabletAggregate.get(TabletStatisticType.SIZE);
        StatisticalSummary tabletTime = tabletAggregate.get(TabletStatisticType.TIME);
        long tabletMax = unit.convert((long) tabletTime.getMax(), TimeUnit.MILLISECONDS);
        long tabletMean = unit.convert((long) tabletTime.getMean(), TimeUnit.MILLISECONDS);
        Collection<TabletStatistic> topOldestMetrics = summary.findTabletPrefixesByOldest(DEFAULT_TOP_COUNT);
        Collection<TabletStatistic> topSizeMetrics = summary.findTabletPrefixesByLargest(DEFAULT_TOP_COUNT);

        sb.append(String.format("Tablets { prefix: %d, total: %d } %s", summary.totalTabletPrefixes(),
                summary.totalTablets(), System.lineSeparator()));
        sb.append(System.lineSeparator());
        sb.append(String.format("Size: { sum: %s, avg: %s } %s",
                FileUtils.byteCountToDisplaySize((long) tabletSizes.getSum()),
                FileUtils.byteCountToDisplaySize((long) tabletSizes.getMean()), System.lineSeparator()));

        sb.append(String.format("Age: { max: %d, avg: %d } %s", tabletMax, tabletMean, System.lineSeparator()));

        sb.append(System.lineSeparator());
        sb.append(System.lineSeparator());

        sb.append(String.format("Top %d Tablets (by-age) %s", DEFAULT_TOP_COUNT, System.lineSeparator()));
        sb.append("---");
        sb.append(System.lineSeparator());

        for (TabletStatistic entry : topOldestMetrics) {
            sb.append(String.format("%s = { tablets: %d, max-age: %d } %s", entry.getKeyName(), entry.getKeyCount(),
                    unit.convert(entry.maxAge(), TimeUnit.MILLISECONDS), System.lineSeparator()));
        }

        sb.append(System.lineSeparator());
        sb.append(System.lineSeparator());

        sb.append(String.format("Top %d Tablets (by-size) %s", DEFAULT_TOP_COUNT, System.lineSeparator()));
        sb.append("---");
        sb.append(System.lineSeparator());

        for (TabletStatistic entry : topSizeMetrics) {
            long totalSize = (long) entry.getSummary(TabletStatisticType.SIZE).getSum();
            sb.append(String.format("%s = { tablets: %d, size: %s } %s", entry.getKeyName(), entry.getKeyCount(),
                    FileUtils.byteCountToDisplaySize(totalSize), System.lineSeparator()));
        }

        sb.append(System.lineSeparator());
        sb.append(System.lineSeparator());

        sb.append(String.format("Tablets (by-name) %s", System.lineSeparator()));
        sb.append("---");
        sb.append(System.lineSeparator());

        for (TabletStatistic entry : summary.getSummary()) {
            long totalSize = (long) entry.getSummary(TabletStatisticType.SIZE).getSum();
            sb.append(String.format("%s = { tablets: %d, size: %s, max-age: %d } %s", entry.getKeyName(),
                    entry.getKeyCount(), FileUtils.byteCountToDisplaySize(totalSize),
                    unit.convert(entry.maxAge(), TimeUnit.MILLISECONDS), System.lineSeparator()));
        }

        sb.append(System.lineSeparator());
        sb.append(System.lineSeparator());

        return sb.toString();
    }
}
