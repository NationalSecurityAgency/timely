package timely.store.cache;

import java.util.Iterator;
import java.util.List;

public class WrappedGorillaDecompressorIterator implements Iterator<WrappedGorillaDecompressor> {

    private TaggedMetric taggedMetric;
    private List<WrappedGorillaDecompressor> decompressors;
    private Iterator<WrappedGorillaDecompressor> iterator;
    private WrappedGorillaDecompressor current = null;

    public WrappedGorillaDecompressorIterator(TaggedMetric taggedMetric, List<WrappedGorillaDecompressor> decompressors) {
        this.taggedMetric = taggedMetric;
        this.decompressors = decompressors;
        this.iterator = this.decompressors.iterator();
    }

    public WrappedGorillaDecompressor getDecompressorWrapper() {
        if (current == null) {
            if (iterator.hasNext()) {
                current = iterator.next();
            }
        }
        return current;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public WrappedGorillaDecompressor next() {
        current = iterator.next();
        return current;
    }

    public TaggedMetric getTaggedMetric() {
        return taggedMetric;
    }
}