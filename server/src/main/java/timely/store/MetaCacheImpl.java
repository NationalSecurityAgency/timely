package timely.store;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import timely.Configuration;
import timely.api.model.Meta;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class MetaCacheImpl implements MetaCache {

    private static final Object DUMMY = new Object();
    private volatile boolean closed = false;
    private Cache<Meta, Object> cache = null;

    @Override
    public void init(Configuration config) {
        String expireMinutes = config.get(Configuration.META_CACHE_EXPIRATION);
        long defaultExpiration = Configuration.META_CACHE_EXPIRATION_DEFAULT;
        if (!StringUtils.isEmpty(expireMinutes)) {
            defaultExpiration = Long.parseLong(expireMinutes);
        }
        String cap = config.get(Configuration.META_CACHE_INITIAL_CAPACITY);
        int initialCapacity = Configuration.META_CACHE_INITIAL_CAPACITY_DEFAULT;
        if (!StringUtils.isEmpty(cap)) {
            initialCapacity = Integer.parseInt(cap);
        }
        String max = config.get(Configuration.META_CACHE_MAX_CAPACITY);
        long maxCapacity = Configuration.META_CACHE_MAX_CAPACITY_DEFAULT;
        if (!StringUtils.isEmpty(max)) {
            maxCapacity = Long.parseLong(max);
        }
        cache = Caffeine.newBuilder().expireAfterAccess(defaultExpiration, TimeUnit.MINUTES)
                .initialCapacity(initialCapacity).maximumSize(maxCapacity).build();
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
