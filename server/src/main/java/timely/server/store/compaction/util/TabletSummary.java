package timely.server.store.compaction.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.math3.stat.descriptive.AggregateSummaryStatistics;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import timely.server.store.compaction.TabletRowAdapter;

public class TabletSummary {

    private final Collection<TabletStatistic> tabletStats;

    private TabletSummary(Collection<TabletStatistic> tabletStats) {
        this.tabletStats = tabletStats;
    }

    public static Builder newBuilder(Collection<MetadataAccumulator.Entry> entries) {
        return new Builder(entries);
    }

    public Map<TabletStatisticType,StatisticalSummary> aggregateSummary() {
        // @formatter:off
        return tabletStats.stream()
                .flatMap(m -> m.getSummaryMap().entrySet().stream())
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())))
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, v -> AggregateSummaryStatistics.aggregate(v.getValue())));
    }

    public long totalTabletPrefixes() {
        return tabletStats.size();
    }

    public long totalTablets() {
        // @formatter:off
        return tabletStats.stream()
                .mapToLong(TabletStatistic::getKeyCount)
                .sum();
    }

    public Collection<TabletStatistic> findTabletPrefixesByOldest(int count) {
        // @formatter:off
        return tabletStats.stream()
                .sorted(Comparator.comparingLong(TabletStatistic::maxAge)
                        .reversed()
                        .thenComparing(TabletStatistic::getKeyName))
                .limit(count)
                .collect(Collectors.toList());
    }

    public Collection<TabletStatistic> findTabletPrefixesByLargest(int count) {
        // @formatter:off
        return tabletStats.stream()
                .sorted(Comparator.comparingLong(TabletStatistic::totalSize)
                        .reversed()
                        .thenComparing(TabletStatistic::getKeyName))
                .limit(count)
                .collect(Collectors.toList());
    }

    public Collection<TabletStatistic> getSummary() {
        return tabletStats;
    }

    public static class Builder {
        private final Collection<MetadataAccumulator.Entry> entries;
        private long explictTimeMillis;
        private boolean useExplicitTime;
        private boolean disableTabletRowCheckFilter;

        public Builder(Collection<MetadataAccumulator.Entry> entries) {
            this.entries = entries;
        }

        public Builder currentTime(long millis) {
            explictTimeMillis = millis;
            useExplicitTime = true;
            return this;
        }

        public Builder disableTabletRowCheckFilter() {
            disableTabletRowCheckFilter = true;
            return this;
        }

        public TabletSummary build() {
            Stream<MetadataAccumulator.Entry> stream = entries.stream();
            if (!disableTabletRowCheckFilter) {
                stream = stream.filter(Builder::tabletEndAnPrevAreSame);
            }

            // @formatter:off
            Collection<TabletStatistic> tabletStats = stream
                    .collect(Collectors.groupingBy(MetadataAccumulator.Entry::getTabletPrefix)).entrySet()
                    .stream()
                    .map(m -> computeStatistic(m.getValue(), m.getKey()))
                    .sorted(Comparator.comparing(TabletStatistic::getKeyName))
                    .collect(Collectors.toList());

            // @formatter:on
            return new TabletSummary(tabletStats);
        }

        private TabletStatistic computeStatistic(Collection<MetadataAccumulator.Entry> entries, String keyName) {
            Map<TabletStatisticType,SummaryStatistics> stats = new HashMap<>(TabletStatisticType.values().length);
            int maxSize = entries.size();
            long timeMillis = useExplicitTime ? explictTimeMillis : System.currentTimeMillis();
            for (TabletStatisticType statType : TabletStatisticType.values()) {
                SummaryStatistics summaryStat = new SummaryStatistics();
                Iterator<MetadataAccumulator.Entry> iterator = entries.iterator();
                int idx = 0;
                while (iterator.hasNext() && idx < maxSize) {
                    MetadataAccumulator.Entry e = iterator.next();
                    double d;
                    switch (statType) {
                        case SIZE:
                            d = e.getTotalFileBytes();
                            break;
                        case TIME:
                            // check the row key offset first; this was done to support
                            // tests so that they age could function from the row-key
                            // the tablets that do not have a key will use the modification time
                            d = timeMillis - e.getTabletOffset().orElse(e.getMilliseconds());
                            break;
                        default:
                            throw new IllegalStateException("Undefined switch case: " + statType.name());
                    }
                    summaryStat.addValue(d);
                    idx++;
                }
                stats.put(statType, summaryStat);
            }

            return new TabletStatistic(keyName, entries.size(), stats);
        }

        private static boolean tabletEndAnPrevAreSame(MetadataAccumulator.Entry entry) {
            if ((null == entry.getTablet()) || (null == entry.getTabletPrev())) {
                return false;
            }
            Optional<String> o1 = TabletRowAdapter.decodeRowPrefix(entry.getTablet());
            Optional<String> o2 = TabletRowAdapter.decodeRowPrefix(entry.getTabletPrev());
            return o1.equals(o2);
        }
    }
}
