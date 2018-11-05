package timely.store.cache;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.request.timeseries.QueryRequest;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

public class DataStoreCacheIterator implements SortedKeyValueIterator<Key, Value> {

    private static final Logger LOG = LoggerFactory.getLogger(DataStoreCacheIterator.class);
    private DataStoreCache store;
    private VisibilityFilter visibilityFilter;
    private QueryRequest.SubQuery query;
    private long startTs;
    private long endTs;

    private Iterator<Map.Entry<TaggedMetric, GorillaStore>> storeItr = null;
    private WrappedGorillaDecompressorIterator decompressors = null;
    private KeyValue currentKeyValue = null;
    private Queue<KeyValue> kvQueue = new LinkedList<>();
    private Set<TaggedMetric> matchingTaggedMetrics = new HashSet<>();

    public DataStoreCacheIterator(DataStoreCache store, VisibilityFilter visibilityFilter, QueryRequest.SubQuery query,
            long startTs, long endTs) {

        this.store = store;
        this.visibilityFilter = visibilityFilter;
        this.query = query;
        this.startTs = startTs;
        this.endTs = endTs;
        Map<TaggedMetric, GorillaStore> storeMap = this.store.getGorillaStores(query.getMetric());
        this.storeItr = storeMap.entrySet().iterator();
        this.decompressors = getNextDecompressorIterable();

        long start = System.currentTimeMillis();
        Map<Key, Value> entries = getEntries();
        for (Map.Entry<Key, Value> entry : entries.entrySet()) {
            kvQueue.add(new KeyValue(entry.getKey(), entry.getValue()));
        }
        LOG.info(
                "Time to initialize cache iterator for {} with {} TaggedMetric/GorillaStore pairs and {} K/V entries - {}ms",
                query.toString(), storeMap.size(), entries.size(), System.currentTimeMillis() - start);
    }

    private WrappedGorillaDecompressorIterator getNextDecompressorIterable() {
        Map<String, String> requestedTags = query.getTags();
        WrappedGorillaDecompressorIterator nextPair = null;
        while (nextPair == null && storeItr.hasNext()) {
            Map.Entry<TaggedMetric, GorillaStore> entry = storeItr.next();
            TaggedMetric currentTaggedMetric = entry.getKey();

            // keep a cache of tagged metrics that match
            boolean addCurrent = false;
            if (matchingTaggedMetrics.contains(currentTaggedMetric)) {
                addCurrent = true;
            } else {
                if (currentTaggedMetric.matches(requestedTags) && entry.getKey().isVisible(visibilityFilter)) {
                    addCurrent = true;
                    matchingTaggedMetrics.add(currentTaggedMetric);
                }
            }

            if (addCurrent) {
                List<WrappedGorillaDecompressor> listDecompressors = entry.getValue().getDecompressors(startTs, endTs);
                if (listDecompressors.size() > 0) {
                    nextPair = new WrappedGorillaDecompressorIterator(currentTaggedMetric, listDecompressors);
                }
            }
        }
        return nextPair;
    }

    private Map<Key, Value> getEntries() {

        // sort all retrieved entries by key order, consistent with the accumulo
        // version
        Map<Key, Value> entries = new TreeMap<>();
        if (decompressors != null) {
            TaggedMetric tm = decompressors.getTaggedMetric();
            WrappedGorillaDecompressor decompressor = decompressors.getDecompressorWrapper();

            fi.iki.yak.ts.compression.gorilla.Pair gPair = null;
            while (decompressor != null && decompressors != null) {
                gPair = decompressor.readPair();
                if (gPair == null) {
                    if (decompressors.hasNext()) {
                        decompressor = decompressors.next();
                    } else {
                        if ((decompressors = getNextDecompressorIterable()) != null) {
                            tm = decompressors.getTaggedMetric();
                            decompressor = decompressors.getDecompressorWrapper();
                        }
                    }
                } else {
                    long ts = gPair.getTimestamp();
                    if (ts >= startTs && ts <= endTs) {
                        entries.put(MetricAdapter.toKey(this.query.getMetric(), tm.getTags(), ts), new Value(
                                MetricAdapter.encodeValue(gPair.getDoubleValue())));
                    }
                }
            }
        }
        return entries;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {

    }

    @Override
    public boolean hasTop() {
        return currentKeyValue != null;
    }

    @Override
    public void next() throws IOException {
        currentKeyValue = kvQueue.poll();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        currentKeyValue = kvQueue.poll();
    }

    @Override
    public Key getTopKey() {
        if (currentKeyValue != null) {
            return currentKeyValue.getKey();
        } else {
            return null;
        }
    }

    @Override
    public Value getTopValue() {
        if (currentKeyValue != null) {
            return currentKeyValue.getValue();
        } else {
            return null;
        }
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return null;
    }

}
