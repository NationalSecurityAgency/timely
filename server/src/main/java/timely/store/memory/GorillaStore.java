package timely.store.memory;

import fi.iki.yak.ts.compression.gorilla.*;

import java.util.*;

public class GorillaStore {

    private Queue<CompressorWrapper> archivedCompressors = new LinkedList<CompressorWrapper>();

    private CompressorWrapper current = null;

    private CompressorWrapper getCompressor(long timestamp) {
        if (current == null) {
            synchronized (this) {
                current = new CompressorWrapper();
                current.setOldestTimestamp(timestamp);
                current.setNewestTimestamp(timestamp);
                current.setCompressorOutput(new LongArrayOutput(480));
                current.setCompressor(new GorillaCompressor(timestamp, current.getCompressorOutput()));
            }
        }
        return current;
    }

    private void closeBitOutput(BitOutput out) {
        out.writeBits(0x0F, 4);
        out.writeBits(0xFFFFFFFF, 32);
        out.skipBit();
        out.flush();
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
                BitOutput copyLongArrayOutput = current.getCompressorOutput();
                closeBitOutput(copyLongArrayOutput);

                LongArrayInput decompressorByteBufferInput = new LongArrayInput(
                        ((LongArrayOutput) copyLongArrayOutput).getLongArray());
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

    public long getNewestTimestamp() {
        long newestTimestamp = 0;
        synchronized (this) {
            newestTimestamp = current.getNewestTimestamp();
        }
        return newestTimestamp;
    }

    public long getOldestTimestamp() {
        long first = 0;
        synchronized (this) {
            first = current.getOldestTimestamp();
        }
        for (CompressorWrapper c : archivedCompressors) {
            if (c.getOldestTimestamp() < first) {
                first = c.getOldestTimestamp();
            }
        }
        return first;
    }
}
