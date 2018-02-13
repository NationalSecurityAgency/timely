package timely.store.memory;

import java.util.Iterator;
import java.util.List;

public class DecompressorWrapperListIterable {

    private TaggedMetric taggedMetric;
    private List<DecompressorWrapper> decompressors;
    private Iterator<DecompressorWrapper> iterator;
    private DecompressorWrapper current = null;

    public DecompressorWrapperListIterable(TaggedMetric taggedMetric, List<DecompressorWrapper> decompressors) {
        this.taggedMetric = taggedMetric;
        this.decompressors = decompressors;
        this.iterator = this.decompressors.iterator();
    }

    public DecompressorWrapper getDecompressorWrapper() {
        if (current == null) {
            if (iterator.hasNext()) {
                current = iterator.next();
            }
        }
        return current;
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public DecompressorWrapper next() {
        current = iterator.next();
        return current;
    }

    public TaggedMetric getTaggedMetric() {
        return taggedMetric;
    }
}