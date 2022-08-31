package timely.server.store.compaction.util;

import java.util.Map;

import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class TabletStatistic {

    private final String keyName;
    private final int keyCount;
    private final Map<TabletStatisticType,SummaryStatistics> stats;
    private final SummaryStatistics timeStat;
    private final SummaryStatistics sizeStat;

    TabletStatistic(String keyName, int keyCount, Map<TabletStatisticType,SummaryStatistics> stats) {
        this.keyName = keyName;
        this.keyCount = keyCount;
        this.stats = stats;

        timeStat = stats.getOrDefault(TabletStatisticType.TIME, new SummaryStatistics());
        sizeStat = stats.getOrDefault(TabletStatisticType.SIZE, new SummaryStatistics());
    }

    public String getKeyName() {
        return keyName;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public StatisticalSummary getSummary(TabletStatisticType statType) {
        return this.stats.getOrDefault(statType, new SummaryStatistics());
    }

    public Map<TabletStatisticType,SummaryStatistics> getSummaryMap() {
        return stats;
    }

    public long maxAge() {
        return (long) timeStat.getMax();
    }

    public long totalSize() {
        return (long) sizeStat.getSum();
    }

    @Override
    public String toString() {
        return String.format("key: %s, count: %d, age: %d, size %d", keyName, keyCount, maxAge(), totalSize());
    }
}
