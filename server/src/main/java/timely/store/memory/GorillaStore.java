package timely.store.memory;

import fi.iki.yak.ts.compression.gorilla.*;

import java.util.*;

public class GorillaStore {

    private Queue<CompressorWrapper> archivedCompressors = new LinkedList<CompressorWrapper>();

    private CompressorWrapper current = null;

    // private Map<Long, Long> checkedOutDecompressors = new TreeMap<>();

    private CompressorWrapper getCompressor(long timestamp) {
        if (current == null) {
            synchronized (this) {
                current = new CompressorWrapper();
                current.setFirstTimestamp(timestamp);
                current.setLastTimestamp(timestamp);
                current.setCompressorOutput(new LongArrayOutput(480));
                current.setCompressor(new GorillaCompressor(timestamp, current.getCompressorOutput()));
            }
        }
        return current;
    }

    public List<DecompressorWrapper> getDecompressors(long begin, long end) {

        List<DecompressorWrapper> decompressors = new ArrayList<>();

        for (CompressorWrapper r : archivedCompressors) {
            if (r.inRange(begin, end)) {
                GorillaDecompressor d = new GorillaDecompressor(new LongArrayInput(
                        ((LongArrayOutput) r.getCompressorOutput()).getLongArray()));
                // use -1 length since this compressor is closed
                decompressors.add(new DecompressorWrapper(d, -1));
            }
        }

        // as long as begin is inRange, we should use the data in the current
        // compressor as well
        synchronized (this) {
            if (current.inRange(begin, begin)) {
                LongArrayInput decompressorByteBufferInput = new LongArrayInput(
                        ((LongArrayOutput) current.getCompressorOutput()).getLongArray());
                GorillaDecompressor d = new GorillaDecompressor(decompressorByteBufferInput);
                decompressors.add(new DecompressorWrapper(d, current.getNumEntries()));
            }
        }
        return decompressors;
    }

    public void addValue(long timestamp, double value) {

        synchronized (this) {
            getCompressor(timestamp).addValue(timestamp, value);
        }
    }

}
