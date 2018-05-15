package timely.store.cache;

import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.LongArrayInput;
import fi.iki.yak.ts.compression.gorilla.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

public class TestWrappedGorillaCompressor {

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {

        long start = System.currentTimeMillis();
        WrappedGorillaCompressor originalCompressor = new WrappedGorillaCompressor(start);
        long t = start;

        for (int x = 1; x <= 10; x++) {
            originalCompressor.addValue(t, 10);
            t = t + 1000;
        }
        originalCompressor.close();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(outputStream);
        oos.writeObject(originalCompressor);
        oos.close();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(inputStream);
        WrappedGorillaCompressor copyCompressor = (WrappedGorillaCompressor) ois.readObject();

        GorillaDecompressor d = new GorillaDecompressor(new LongArrayInput(copyCompressor.getCompressorOutput()));

        LinkedList<Pair> q = new LinkedList<>();
        Pair p = null;
        while ((p = d.readPair()) != null) {
            q.add(p);
        }

        Assert.assertEquals(10, q.size());
        Assert.assertEquals(start, q.peekFirst().getTimestamp());
        Assert.assertEquals(start + 9000, q.peekLast().getTimestamp());
    }

    @Test
    public void testHDFSWrite() throws Exception {

        try {
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://localhost:8020"), configuration);
            GorillaStore store = new GorillaStore(fs, "mymetric", new timely.Configuration());

            long start = System.currentTimeMillis();
            WrappedGorillaCompressor originalCompressor = new WrappedGorillaCompressor(start);
            long t = start;

            for (int x = 1; x <= 10; x++) {
                originalCompressor.addValue(t, 10);
                t = t + 1000;
            }
            originalCompressor.close();

            store.writeCompressor("mymetric", originalCompressor);

            List<WrappedGorillaCompressor> archived = store.readCompressors(fs, new Path("/timely/cache/mymetric"));

            for (WrappedGorillaCompressor c : archived) {

                GorillaDecompressor d = new GorillaDecompressor(new LongArrayInput(c.getCompressorOutput()));
                LinkedList<Pair> q = new LinkedList<>();
                Pair p = null;
                while ((p = d.readPair()) != null) {
                    q.add(p);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
