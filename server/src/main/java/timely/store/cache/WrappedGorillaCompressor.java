package timely.store.cache;

import fi.iki.yak.ts.compression.gorilla.GorillaCompressor;
import fi.iki.yak.ts.compression.gorilla.LongArrayOutput;
import org.apache.commons.lang3.Range;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class WrappedGorillaCompressor implements Serializable {

    private boolean closed = false;
    private long numEntries = 0;
    private long oldestTimestamp;
    private long newestTimestamp;
    private LongArrayOutput compressorOutput = null;
    private long[] backingArray = null;
    private GorillaCompressor compressor;

    public WrappedGorillaCompressor() {

    }

    public WrappedGorillaCompressor(long timestamp) {
        this.compressorOutput = new LongArrayOutput(256);
        this.compressor = new GorillaCompressor(timestamp, this.compressorOutput);
        this.oldestTimestamp = timestamp;
        this.newestTimestamp = timestamp;
    }

    private void closeBitOutput(LongArrayOutput out) {
        out.writeBits(0x0F, 4);
        out.writeBits(0xFFFFFFFF, 32);
        out.skipBit();
        out.flush();
    }

    public long[] getCompressorOutput() {
        if (closed) {
            // long[] copy = new long[backingArray.length];
            // System.arraycopy(backingArray, 0, copy, 0, backingArray.length);
            // return copy;
            return backingArray;
        } else {
            return compressorOutput.getLongArray();
        }
    }

    public long getOldestTimestamp() {
        return oldestTimestamp;
    }

    public long getNewestTimestamp() {
        return newestTimestamp;
    }

    public boolean inRange(long begin, long end) {
        Range<Long> requestedRange = Range.between(begin, end);
        Range<Long> compressorRange = Range.between(oldestTimestamp, newestTimestamp);
        return compressorRange.isOverlappedBy(requestedRange);
    }

    public long getNumEntries() {
        return numEntries;
    }

    public void addValue(long timestamp, double value) {
        if (closed) {
            throw new IllegalStateException("Compressor is closed");
        }
        numEntries++;
        System.out.println("writing to WrappedGorillaDecompressor numEntries=" + numEntries + " " + this);
        newestTimestamp = timestamp;
        compressor.addValue(timestamp, value);
    }

    public void close() {
        compressor.close();
        backingArray = compressorOutput.getLongArray();
        compressorOutput = null;
        closed = true;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        if (!closed) {
            throw new IllegalStateException("Can not serialize compressor before it's closed");
        }
        out.writeLong(numEntries);
        out.writeLong(oldestTimestamp);
        out.writeLong(newestTimestamp);
        int length = backingArray.length;
        out.writeInt(length);
        for (int x = 0; x < length; x++) {
            out.writeLong(backingArray[x]);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        closed = true;
        numEntries = in.readLong();
        oldestTimestamp = in.readLong();
        newestTimestamp = in.readLong();
        int length = in.readInt();
        backingArray = new long[length];
        for (int x = 0; x < length; x++) {
            backingArray[x] = in.readLong();
        }
    }
}
