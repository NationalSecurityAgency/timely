package timely.server.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.LongArrayInput;
import fi.iki.yak.ts.compression.gorilla.Pair;
import timely.server.store.cache.WrappedGorillaCompressor;

public class TestWrappedGorillaCompressor {

    private static final Logger log = LoggerFactory.getLogger(TestWrappedGorillaCompressor.class);

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
        Pair p;
        while ((p = d.readPair()) != null) {
            q.add(p);
        }

        Assert.assertEquals(10, q.size());
        Assert.assertEquals(start, q.peekFirst().getTimestamp());
        Assert.assertEquals(start + 9000, q.peekLast().getTimestamp());
    }
}
