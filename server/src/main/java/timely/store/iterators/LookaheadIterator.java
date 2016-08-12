package timely.store.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Accumulo iterator that wraps another iterator and implements a lookahead
 * feature. As you are iterating over the keys and values, you can peek ahead to
 * the next key/value. Be aware that calling {@link #next()} after
 * {@link #peek()} will return the same key and value. Additionally, you must
 * consume both the key and value after calling {@link #next()} to clear the
 * internal state. Example usage:
 * 
 * <pre>
 * LookaheadIterator iter = new LookaheadIterator(source);
 * iter.seek();
 * iter.hasTop();
 * iter.getTopKey();
 * iter.getTopValue();
 * 
 * // Lookahead at the next key/value
 * KeyValuePair lookahead = iter.peek();
 * lookahead.getKey();
 * lookahead.getValue();
 * 
 * // Move ahead to the next value, which is really the last value because of the
 * // peek.
 * iter.next();
 * iter.hasTop();
 * iter.getTopKey();
 * iter.getTopValue(); // must call getTopKey and getTopValue after a peek to clear
 * // the internal state
 * 
 * iter.next();
 * iter.hasTop();
 * iter.getTopKey();
 * iter.getTopValue();
 * </pre>
 */
public class LookaheadIterator implements SortedKeyValueIterator<Key, Value> {

    private final SortedKeyValueIterator<Key, Value> source;
    private KeyValuePair lookahead = new KeyValuePair();
    private volatile boolean sourceEmpty = false;

    public LookaheadIterator(SortedKeyValueIterator<Key, Value> source) {
        this.source = source;
    }

    public SortedKeyValueIterator<Key, Value> getSource() {
        return source;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        this.source.init(source, options, env);
        lookahead.empty();
        sourceEmpty = false;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        source.seek(range, columnFamilies, inclusive);
    }

    @Override
    public void next() throws IOException {
        if (sourceEmpty) {
            return;
        }
        if (lookahead.isEmpty()) {
            source.next();
        }
    }

    @Override
    public boolean hasTop() {
        if (!lookahead.isEmpty()) {
            return true;
        }
        if (sourceEmpty) {
            return false;
        }
        boolean result = source.hasTop();
        sourceEmpty = !result;
        return result;
    }

    @Override
    public Key getTopKey() {
        if (!lookahead.isEmpty()) {
            Key k = lookahead.getKey();
            if (null == k) {
                throw new IllegalStateException("Must call next to retrieve following key.");
            }
            lookahead.setKey(null);
            return k;
        }
        return source.getTopKey();
    }

    @Override
    public Value getTopValue() {
        if (!lookahead.isEmpty()) {
            Value v = lookahead.getValue();
            if (null == v) {
                throw new IllegalStateException("Must call next to retrieve following value.");
            }
            lookahead.setValue(null);
            return v;
        }
        return source.getTopValue();
    }

    public KeyValuePair peek() throws IOException {
        lookahead();
        return sourceEmpty ? null : lookahead;
    }

    private void lookahead() throws IOException {
        if (sourceEmpty || !lookahead.isEmpty()) {
            return;
        }
        source.next();
        if (source.hasTop()) {
            this.lookahead.setKey(source.getTopKey());
            this.lookahead.setValue(source.getTopValue());
        } else {
            this.sourceEmpty = true;
            this.lookahead.empty();
        }
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        throw new UnsupportedOperationException();
    }

}
