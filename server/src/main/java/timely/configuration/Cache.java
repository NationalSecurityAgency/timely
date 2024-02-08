package timely.configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import timely.store.cache.DataStoreCache;

public class Cache {

    private boolean enabled = false;
    private HashMap<String,Integer> metricAgeOffHours = new HashMap<>();
    private List<String> nonCachedMetrics = new ArrayList<>();
    private long maxUniqueTagSets = 50000;
    private long flushInterval = 5000;
    private long staleCacheExpiration = 1800000;

    public HashMap<String,Integer> getMetricAgeOffHours() {
        return metricAgeOffHours;
    }

    public void setMetricAgeOffHours(HashMap<String,Integer> metricAgeOffHours) {
        this.metricAgeOffHours = metricAgeOffHours;
    }

    public void setDefaultAgeOffHours(int defaultAgeOffHours) {
        this.metricAgeOffHours.put(DataStoreCache.DEFAULT_AGEOFF_KEY, defaultAgeOffHours);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<String> getNonCachedMetrics() {
        return nonCachedMetrics;
    }

    public void setNonCachedMetrics(List<String> nonCachedMetrics) {
        this.nonCachedMetrics = nonCachedMetrics;
    }

    public long getMaxUniqueTagSets() {
        return maxUniqueTagSets;
    }

    public void setMaxUniqueTagSets(long maxUniqueTagSets) {
        this.maxUniqueTagSets = maxUniqueTagSets;
    }

    public long getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(long flushInterval) {
        this.flushInterval = flushInterval;
    }

    public long getStaleCacheExpiration() {
        return staleCacheExpiration;
    }

    public void setStaleCacheExpiration(long staleCacheExpiration) {
        this.staleCacheExpiration = staleCacheExpiration;
    }
}
