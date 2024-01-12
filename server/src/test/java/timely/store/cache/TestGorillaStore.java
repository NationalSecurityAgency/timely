package timely.store.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import fi.iki.yak.ts.compression.gorilla.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import timely.configuration.Configuration;

public class TestGorillaStore {

    static private Configuration configuration = null;

    @BeforeClass
    public static void setup() {
        configuration = new Configuration();
        configuration.getSecurity().setAllowAnonymousHttpAccess(true);
    }

    @Test
    public void testGorillaStore() {

        GorillaStore gStore = new GorillaStore("", Long.MAX_VALUE);

        Double[] valueArray = new Double[] { 1.123, 2.314, 3.856, 4.7678, 5.8966, 6.0976, 1.2345, 2.3456, 3.4567 };
        List<Double> values = new ArrayList<>(Arrays.asList(valueArray));

        long now = System.currentTimeMillis();
        for (Double d : values) {
            gStore.addValue(now += 100, d);
        }

        List<WrappedGorillaDecompressor> decompressorList = gStore.getDecompressors(0, Long.MAX_VALUE);
        Pair pair;
        int x = 0;
        for (WrappedGorillaDecompressor w : decompressorList) {
            while ((pair = w.readPair()) != null) {
                Assert.assertEquals(values.get(x++).doubleValue(), pair.getDoubleValue(), 0);
            }
        }
    }

    @Test
    public void testExtentOfStorage() {

        GorillaStore gStore = new GorillaStore("", Long.MAX_VALUE);

        HashMap<String, String> tags = new HashMap<>();
        tags.put("host", "localhost");

        long start = System.currentTimeMillis();
        long timestamp = start;

        for (int x = 1; x <= 100; x++) {

            gStore.addValue(timestamp, 2.0);
            timestamp = timestamp + 1000;

            if (x % 10 == 0) {
                gStore.archiveCurrentCompressor();
            }
            if (x < 50) {
                continue;
            }

            long totalObservations = 0;

            List<WrappedGorillaDecompressor> decompressorList = gStore.getDecompressors(start, timestamp);
            for (WrappedGorillaDecompressor w : decompressorList) {
                while (w.readPair() != null) {
                    totalObservations++;
                }
            }
            Assert.assertEquals("Unexpected number of total observations", x, totalObservations);
        }
    }

    @Test
    public void testArchive() {

        GorillaStore gStore = new GorillaStore("", Long.MAX_VALUE);

        gStore.addValue(1, 1.123);
        gStore.addValue(2, 2.314);
        gStore.archiveCurrentCompressor();
        Assert.assertEquals(1, gStore.getOldestTimestamp());
        gStore.addValue(3, 3.856);
        gStore.addValue(4, 4.7678);
        gStore.archiveCurrentCompressor();
        gStore.ageOffArchivedCompressors(2, 5);
        Assert.assertEquals(3, gStore.getOldestTimestamp());
    }
}
