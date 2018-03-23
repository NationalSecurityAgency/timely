package timely.store.cache;

import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.Pair;

public class WrappedGorillaDecompressor {

    private GorillaDecompressor decompressor;
    private long length;
    private long numRead = 0;

    public WrappedGorillaDecompressor(GorillaDecompressor decompressor, long length) {

        this.decompressor = decompressor;
        this.length = length;
        System.out.println("create WrappedGorillaDecompressor length=" + length + " " + this);
    }

    public Pair readPair() {
        if (length == -1 || numRead < length) {
            System.out.println("reading from WrappedGorillaDecompressor numRead=" + numRead + " length=" + length + " "
                    + this);
            numRead++;
            return decompressor.readPair();
        } else {
            return null;
        }
    }
}
