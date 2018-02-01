package timely.store.memory;

import fi.iki.yak.ts.compression.gorilla.*;

public class GorillaStore {

    GorillaCompressor compressor = null;
    BitOutput compressorOutput = null;

    private GorillaCompressor getCompressor(long timestamp) {
        if (compressor == null) {
            compressorOutput = new LongArrayOutput(480);
            compressor = new GorillaCompressor(timestamp, compressorOutput);
        }
        return compressor;
    }

    public GorillaDecompressor getDecompressor() {
        if (compressor == null) {
            return null;
        }
        LongArrayOutput newCompressorOutput = null;
        synchronized (this) {
            newCompressorOutput = new LongArrayOutput((LongArrayOutput) compressorOutput);
        }

        // same code that the compressor uses in close()
        newCompressorOutput.writeBits(0x0F, 4);
        newCompressorOutput.writeBits(0xFFFFFFFF, 32);
        newCompressorOutput.skipBit();
        newCompressorOutput.flush();
        LongArrayInput decompressorByteBufferInput = new LongArrayInput(newCompressorOutput.getLongArray());
        return new GorillaDecompressor(decompressorByteBufferInput);
    }

    public void addValue(long timestamp, double value) {
        synchronized (this) {
            GorillaCompressor compressor = getCompressor(timestamp);
            compressor.addValue(timestamp, value);
        }
    }
}
