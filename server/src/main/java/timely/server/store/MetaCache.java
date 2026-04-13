package timely.server.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import timely.common.configuration.MetaCacheProperties;
import timely.common.configuration.TimelyProperties;
import timely.model.Meta;

public final class MetaCache implements Iterable<Meta> {

    private static final Logger log = LoggerFactory.getLogger(MetaCache.class);
    private static final Object DUMMY = new Object();
    private final Cache<Meta,Object> cache;
    private final AccumuloClient accumuloClient;
    private final TimelyProperties timelyProperties;
    private final MetaCacheProperties metaCacheProperties;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    public MetaCache(AccumuloClient accumuloClient, TimelyProperties timelyProperties, MetaCacheProperties metaCacheProperties) {
        this.accumuloClient = accumuloClient;
        this.timelyProperties = timelyProperties;
        this.metaCacheProperties = metaCacheProperties;
        cache = Caffeine.newBuilder().initialCapacity(1000).build();
        long cacheRefreshMinutes = metaCacheProperties.getCacheRefreshMinutes();
        if (cacheRefreshMinutes > 0) {
            executorService.scheduleAtFixedRate(() -> refreshCache(metaCacheProperties.getExpirationMinutes()), 1, cacheRefreshMinutes, TimeUnit.MINUTES);
        }
    }

    private void refreshCache(long expirationMinutes) {
        long oldestTimestamp = System.currentTimeMillis() - (expirationMinutes * 60 * 1000);
        String metaTable = timelyProperties.getMetaTable();
        Map<String,Map<String,Long>> metricMap = new HashMap<>();
        try (Scanner scanner = accumuloClient.createScanner(metaTable)) {
            Map<Meta,Object> newCache = new HashMap<>();
            log.debug("Begin scanning " + metaTable);
            Key metricPrefixBeginKey = new Key(Meta.METRIC_PREFIX);
            int firstChar = Meta.METRIC_PREFIX.charAt(0);
            Key metricPrefixEndKey = new Key((char) firstChar + 1 + ":");
            Range metricRange = new Range(metricPrefixBeginKey, true, metricPrefixEndKey, false);
            scanner.setRange(metricRange);
            Set<String> allMetrics = new TreeSet<>();
            for (Map.Entry<Key,Value> entry : scanner) {
                Meta meta = Meta.parse(entry.getKey(), entry.getValue(), Meta.METRIC_PREFIX);
                allMetrics.add(meta.getMetric());
            }
            for (String currMetric : allMetrics) {
                Key begin = new Key(Meta.VALUE_PREFIX, currMetric);
                Key end = new Key(Meta.VALUE_PREFIX, currMetric + '\0');
                Range range = new Range(begin, true, end, false);
                scanner.setRange(range);
                scanner.setBatchSize(Long.valueOf(metaCacheProperties.getMaxTagValues()).intValue());
                Iterator<Map.Entry<Key,Value>> iter = scanner.iterator();
                boolean maxedOutValues = false;
                while (iter.hasNext() && !maxedOutValues) {
                    Map.Entry<Key,Value> entry = iter.next();
                    Meta meta = Meta.parse(entry.getKey(), entry.getValue(), Meta.VALUE_PREFIX);
                    String metric = meta.getMetric();
                    String tagKey = meta.getTagKey();
                    Map<String,Long> tagMap = metricMap.computeIfAbsent(metric, k -> new HashMap<>());
                    long numTagValues = tagMap.getOrDefault(tagKey, 0L);
                    if (entry.getKey().getTimestamp() > oldestTimestamp) {
                        tagMap.put(tagKey, ++numTagValues);
                        if (numTagValues <= metaCacheProperties.getMaxTagValues()) {
                            newCache.put(meta, DUMMY);
                        } else {
                            // found maxTagValues on this refresh
                            maxedOutValues = true;
                        }
                    }
                }
            }
            synchronized (cache) {
                cache.invalidateAll();
                cache.putAll(newCache);
            }
            log.debug("Finished scanning " + metaTable);
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void add(Meta meta) {
        cache.put(meta, DUMMY);
    }

    public boolean contains(Meta meta) {
        return cache.asMap().containsKey(meta);
    }

    public void addAll(Collection<Meta> c) {
        c.forEach(m -> cache.put(m, DUMMY));
    }

    public void clear() {
        synchronized (cache) {
            cache.invalidateAll();
        }
    }

    @Override
    public Iterator<Meta> iterator() {
        return new TreeSet<>(cache.asMap().keySet()).iterator();
    }

    public void close() {
        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {

        } finally {
            if (!this.executorService.isTerminated()) {
                this.executorService.shutdownNow();
            }
        }
    }

    public MetaCacheProperties getMetaCacheProperties() {
        return metaCacheProperties;
    }
}
