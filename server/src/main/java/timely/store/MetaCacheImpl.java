package timely.store;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.configuration.BaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.model.Meta;
import timely.configuration.Accumulo;
import timely.configuration.Configuration;

public class MetaCacheImpl implements MetaCache {

    private static Logger LOG = LoggerFactory.getLogger(MetaCacheImpl.class);
    private static final Object DUMMY = new Object();
    private volatile boolean closed = false;
    private Cache<Meta, Object> cache = null;
    private Configuration configuration;
    private Timer timer = new Timer("MetaCacheImpl", true);

    @Override
    public void init(Configuration config) {
        configuration = config;
        cache = Caffeine.newBuilder().initialCapacity(1000).build();
        long cacheRefreshMinutes = configuration.getMetaCache().getCacheRefreshMinutes();
        if (cacheRefreshMinutes > 0) {
            timer.schedule(new TimerTask() {

                @Override
                public void run() {
                    refreshCache(config.getMetaCache().getExpirationMinutes());
                }
            }, 60000, cacheRefreshMinutes * 60 * 1000);
        }
    }

    private void refreshCache(long expirationMinutes) {
        long oldestTimestamp = System.currentTimeMillis() - (expirationMinutes * 60 * 1000);
        final BaseConfiguration apacheConf = new BaseConfiguration();
        Accumulo accumuloConf = configuration.getAccumulo();
        apacheConf.setProperty("instance.name", accumuloConf.getInstanceName());
        apacheConf.setProperty("instance.zookeeper.host", accumuloConf.getZookeepers());
        final ClientConfiguration aConf = new ClientConfiguration((Collections.singletonList(apacheConf)));
        String metaTable = configuration.getMetaTable();
        Map<String, Map<String, Long>> metricMap = new HashMap<>();
        try {
            final Instance instance = new ZooKeeperInstance(aConf);
            Connector connector = instance.getConnector(accumuloConf.getUsername(),
                    new PasswordToken(accumuloConf.getPassword()));
            Scanner scanner = connector.createScanner(metaTable, Authorizations.EMPTY);
            Key begin = new Key(Meta.VALUE_PREFIX);
            Key end = new Key("w" + '\0');
            Range range = new Range(begin, true, end, false);
            scanner.setRange(range);
            LOG.debug("Begin scanning " + metaTable);
            for (Map.Entry<Key, Value> entry : scanner) {
                Meta meta = Meta.parse(entry.getKey(), entry.getValue(), Meta.VALUE_PREFIX);
                String metric = meta.getMetric();
                String tagKey = meta.getTagKey();
                Map<String, Long> tagMap = metricMap.get(metric);
                if (tagMap == null) {
                    tagMap = new HashMap<>();
                    metricMap.put(metric, tagMap);
                }
                long numValues = tagMap.getOrDefault(tagKey, 0l);
                if (entry.getKey().getTimestamp() > oldestTimestamp) {
                    tagMap.put(tagKey, ++numValues);
                    if (numValues <= configuration.getMetaCache().getMaxTagValues()) {
                        cache.put(meta, DUMMY);
                    } else {
                        // found maxTagValues on this refresh, so remove this key
                        cache.invalidate(meta);
                    }
                } else {
                    // this entry is more than expirationMinutes old, so remove it
                    cache.invalidate(meta);
                }
            }
            LOG.debug("Finished scanning " + metaTable);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void add(Meta meta) {
        cache.put(meta, DUMMY);
    }

    @Override
    public boolean contains(Meta meta) {
        return cache.asMap().containsKey(meta);
    }

    @Override
    public void addAll(Collection<Meta> c) {
        c.forEach(m -> cache.put(m, DUMMY));
    }

    @Override
    public Iterator<Meta> iterator() {
        return cache.asMap().keySet().iterator();
    }

    @Override
    public void close() {
        this.closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

}
