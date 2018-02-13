package timely.store.memory;

import fi.iki.yak.ts.compression.gorilla.BitOutput;
import fi.iki.yak.ts.compression.gorilla.GorillaCompressor;

public class CompressorWrapper {

    private long firstTimestamp;
    private long lastTimestamp;
    private GorillaCompressor compressor;
    private BitOutput compressorOutput = null;
    public long numEntries = 0;

    public CompressorWrapper() {

    }

    public CompressorWrapper(GorillaCompressor compressor, long firstTimestamp, long lastTimestamp) {
        this.compressor = compressor;
        this.firstTimestamp = firstTimestamp;
        this.lastTimestamp = lastTimestamp;
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

    public void setFirstTimestamp(long firstTimestamp) {
        this.firstTimestamp = firstTimestamp;
    }

    public long getFirstTimestamp() {
        return firstTimestamp;
    }

    public void setLastTimestamp(long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public boolean inRange(long begin, long end) {
        return (begin >= firstTimestamp || end <= lastTimestamp);
    }

    public long getNumEntries() {
        return numEntries;
    }

    public void addValue(long timestamp, double value) {
        numEntries++;
        lastTimestamp = timestamp;
        compressor.addValue(timestamp, value);
    }
}
