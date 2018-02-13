package timely.store.memory;

import fi.iki.yak.ts.compression.gorilla.*;

public class DecompressorWrapper {

    private GorillaDecompressor decompressor;
    private long length;
    private long current = 1;

    public DecompressorWrapper(GorillaDecompressor decompressor, long length) {

        this.decompressor = decompressor;
        this.length = length;
    }

    public void setDecompressor(GorillaDecompressor decompressor) {
        this.decompressor = decompressor;
    }

    public Pair readPair() {
        if (length == -1 || current <= length) {
            current++;
            return decompressor.readPair();
        } else {
            return null;
        }
    }
}
