package timely.store.cache;

import fi.iki.yak.ts.compression.gorilla.BitOutput;
import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.LongArrayInput;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class GorillaStore {

    private Queue<WrappedGorillaCompressor> archivedCompressors = new LinkedList<WrappedGorillaCompressor>();

    transient private WrappedGorillaCompressor current = null;

    private long oldestTimestamp = -1;
    private long newestTimestamp = -1;

    private WrappedGorillaCompressor getCompressor(long timestamp) {
        if (current == null) {
            if (oldestTimestamp == -1) {
                oldestTimestamp = timestamp;
            }
            if (newestTimestamp == -1) {
                newestTimestamp = timestamp;
            }
            synchronized (this) {
                current = new WrappedGorillaCompressor(timestamp);
            }
        }
        return current;
    }

    public int ageOffArchivedCompressors(long maxAge) {
        int numRemoved = 0;
        long oldestRemainingTimestamp = Long.MAX_VALUE;
        synchronized (archivedCompressors) {
            Iterator<WrappedGorillaCompressor> itr = archivedCompressors.iterator();
            long now = System.currentTimeMillis();
            while (itr.hasNext()) {
                WrappedGorillaCompressor c = itr.next();
                if (now - c.getNewestTimestamp() > maxAge) {
                    itr.remove();
                    numRemoved++;
                } else {
                    if (c.getOldestTimestamp() < oldestRemainingTimestamp) {
                        oldestRemainingTimestamp = c.getOldestTimestamp();
                    }
                }
            }
            oldestTimestamp = oldestRemainingTimestamp;
        }
        return numRemoved;
    }

    public void archiveCurrentCompressor() {
        synchronized (this) {
            if (current != null) {
                current.close();
                archivedCompressors.add(current);
                current = null;
            }
        }
    }

    public List<WrappedGorillaDecompressor> getDecompressors(long begin, long end) {

        List<WrappedGorillaDecompressor> decompressors = new ArrayList<>();

        synchronized (archivedCompressors) {
            for (WrappedGorillaCompressor r : archivedCompressors) {
                if (r.inRange(begin, end)) {
                    GorillaDecompressor d = new GorillaDecompressor(new LongArrayInput(r.getCompressorOutput()));
                    // use -1 length since this compressor is closed
                    decompressors.add(new WrappedGorillaDecompressor(d, -1));
                }
            }
        }

        // as long as begin is inRange, we should use the data in the current
        // compressor as well
        synchronized (this) {
            if (current != null && current.inRange(begin, end)) {
                LongArrayInput decompressorByteBufferInput = new LongArrayInput(current.getCompressorOutput());
                GorillaDecompressor d = new GorillaDecompressor(decompressorByteBufferInput);
                decompressors.add(new WrappedGorillaDecompressor(d, current.getNumEntries()));
            }
        }
        return decompressors;
    }

    public void addValue(long timestamp, double value) {

        // values must be inserted in order
        if (timestamp >= newestTimestamp) {
            newestTimestamp = timestamp;
            synchronized (this) {
                getCompressor(timestamp).addValue(timestamp, value);
            }
        }
    }

    public long getNewestTimestamp() {
        return newestTimestamp;
    }

    public long getOldestTimestamp() {
        return oldestTimestamp;
    }
}
