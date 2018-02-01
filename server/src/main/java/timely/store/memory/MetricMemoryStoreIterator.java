package timely.store.memory;

import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.tuple.Pair;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.request.timeseries.QueryRequest;

import java.io.IOException;
import java.util.*;

public class MetricMemoryStoreIterator implements SortedKeyValueIterator<Key,Value> {

    private MetricMemoryStore store;
    private VisibilityFilter visibilityFilter;
    private QueryRequest.SubQuery query;
    private long startTs;
    private long endTs;

    private Iterator<Map.Entry<TaggedMetric, GorillaStore>> storeItr = null;
    private Pair<TaggedMetric, GorillaDecompressor> storePtr = null;
    private GorillaDecompressor currentDecompressor = null;
    private KeyValue currentKeyValue = null;
    private Queue<KeyValue> kvQueue = new LinkedList<>();

    public MetricMemoryStoreIterator(MetricMemoryStore store, VisibilityFilter visibilityFilter, QueryRequest.SubQuery query, long startTs, long endTs) {

        this.store = store;
        this.visibilityFilter = visibilityFilter;
        this.query = query;
        this.startTs = startTs;
        this.endTs = endTs;

        storeItr = store.getGorillaStores(query.getMetric()).entrySet().iterator();
        storePtr = getNextMetricStorePair();
        prepareEntries(100);
    }

    private Pair<TaggedMetric, GorillaDecompressor> getNextMetricStorePair() {
        Map<String, String> requestedTags = query.getTags();
        Pair<TaggedMetric, GorillaDecompressor> nextPair = null;
        while (nextPair == null && storeItr.hasNext()) {
            Map.Entry<TaggedMetric, GorillaStore> entry = storeItr.next();
            if (entry.getKey().matches(requestedTags) && entry.getKey().isVisible(visibilityFilter)) {
                nextPair = Pair.of(entry.getKey(), entry.getValue().getDecompressor());
            }
        }
        return nextPair;
    }


    private void prepareEntries(int bufferSize) {

        if (storePtr != null) {
            TaggedMetric tm = storePtr.getLeft();
            GorillaDecompressor decompressor = storePtr.getRight();

            fi.iki.yak.ts.compression.gorilla.Pair gPair = null;
            while (kvQueue.size() < bufferSize && gPair == null && storePtr != null) {
                gPair = decompressor.readPair();
                if (gPair == null) {
                    if ((storePtr = getNextMetricStorePair()) != null) {
                        tm = storePtr.getLeft();
                        decompressor = storePtr.getRight();
                    }
                } else {
                    if (gPair.getTimestamp() >= startTs && gPair.getTimestamp() <= endTs) {
                        kvQueue.add(new KeyValue(MetricAdapter.toKey(tm.getMetric(), tm.getTags(), gPair.getTimestamp()), new Value(MetricAdapter.encodeValue(gPair.getDoubleValue()))))
                    }
                }
            }
        }
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {

    }

    @Override
    public boolean hasTop() {
        prepareEntries(100);
        return nextKey != null;
    }

    @Override
    public void next() throws IOException {
        return;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        return;
    }

    @Override
    public Key getTopKey() {
        return nextKey;
    }

    @Override
    public Value getTopValue() {
        return nextValue;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return null;
    }
}
