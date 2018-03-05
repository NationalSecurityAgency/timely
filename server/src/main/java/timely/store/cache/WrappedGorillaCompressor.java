package timely.store.cache;

import fi.iki.yak.ts.compression.gorilla.BitOutput;
import fi.iki.yak.ts.compression.gorilla.GorillaCompressor;

public class WrappedGorillaCompressor {

    private long oldestTimestamp;
    private long newestTimestamp;
    private GorillaCompressor compressor;
    private BitOutput compressorOutput = null;
    public long numEntries = 0;

    public WrappedGorillaCompressor() {

    }

    public WrappedGorillaCompressor(GorillaCompressor compressor, long firstTimestamp, long lastTimestamp) {
        this.compressor = compressor;
        this.oldestTimestamp = firstTimestamp;
        this.newestTimestamp = lastTimestamp;
    }

    public GorillaCompressor getCompressor() {
        return compressor;
    }

    public void setCompressor(GorillaCompressor compressor) {
        this.compressor = compressor;
    }

    public BitOutput getCompressorOutput() {
        return compressorOutput;
    }

    public void setCompressorOutput(BitOutput compressorOutput) {
        this.compressorOutput = compressorOutput;
    }

    public void setOldestTimestamp(long oldestTimestamp) {
        this.oldestTimestamp = oldestTimestamp;
    }

    public long getOldestTimestamp() {
        return oldestTimestamp;
    }

    public void setNewestTimestamp(long newestTimestamp) {
        this.newestTimestamp = newestTimestamp;
    }

    public long getNewestTimestamp() {
        return newestTimestamp;
    }

    public boolean inRange(long begin, long end) {
        return (begin >= oldestTimestamp || end <= newestTimestamp);
    }

    public long getNumEntries() {
        return numEntries;
    }

    public void addValue(long timestamp, double value) {
        numEntries++;
        newestTimestamp = timestamp;
        compressor.addValue(timestamp, value);
    }
}
