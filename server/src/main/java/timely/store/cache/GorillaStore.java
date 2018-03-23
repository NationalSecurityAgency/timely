package timely.store.cache;

import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.LongArrayInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

public class GorillaStore {

    private Queue<WrappedGorillaCompressor> archivedCompressors = new LinkedList<WrappedGorillaCompressor>();

    transient private WrappedGorillaCompressor current = null;

    private long oldestTimestamp = -1;
    private long newestTimestamp = -1;

    public GorillaStore() {

    }

    public GorillaStore(FileSystem fs, String metric, timely.Configuration conf) throws IOException {
        String baseDir = "/timely/cache";
        Path directory = new Path(baseDir + "/" + metric);
        List<WrappedGorillaCompressor> compressors = readCompressors(fs, directory);
        archivedCompressors.addAll(compressors);
    }

    private WrappedGorillaCompressor getCompressor(long timestamp) {
        if (current == null) {
            if (oldestTimestamp == -1) {
                oldestTimestamp = timestamp;
            }
            newestTimestamp = timestamp;
            synchronized (this) {
                current = new WrappedGorillaCompressor(timestamp);
            }
        }
        return current;
    }

    public int ageOffArchivedCompressors(long maxAge) {
        int numRemoved = 0;
        long oldestRemainingTimestamp = Long.MAX_VALUE;
        synchronized (archivedCompressors) {
            if (archivedCompressors.size() > 0) {
                Iterator<WrappedGorillaCompressor> itr = archivedCompressors.iterator();
                long now = System.currentTimeMillis();
                while (itr.hasNext()) {
                    WrappedGorillaCompressor c = itr.next();
                    if (now - c.getNewestTimestamp() >= maxAge) {
                        itr.remove();
                        numRemoved++;
                    } else {
                        if (c.getOldestTimestamp() < oldestRemainingTimestamp) {
                            oldestRemainingTimestamp = c.getOldestTimestamp();
                        }
                    }
                }
                if (oldestRemainingTimestamp < Long.MAX_VALUE) {
                    oldestTimestamp = oldestRemainingTimestamp;
                }
            }
        }
        return numRemoved;
    }

    protected void writeCompressor(String metric, WrappedGorillaCompressor wrappedGorillaCompressor) throws IOException {

        try {
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://localhost:8020"), configuration);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmmss.SSS");
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
            String baseDir = "/timely/cache";
            Path directory = new Path(baseDir + "/" + metric);
            String fileName = metric + "-" + sdf.format(new Date(wrappedGorillaCompressor.getOldestTimestamp()));
            Path outputPath = new Path(directory, fileName);
            if (!fs.exists(directory)) {
                fs.mkdirs(directory);
            }
            if (fs.exists(outputPath)) {
                throw new IOException("output path exists");
            }
            OutputStream os = fs.create(outputPath);
            // write object to hdfs file
            ObjectOutputStream oos = new ObjectOutputStream(os);
            oos.writeObject(wrappedGorillaCompressor);
            oos.close();
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    protected List<WrappedGorillaCompressor> readCompressors(FileSystem fs, Path directory) throws IOException {

        List<WrappedGorillaCompressor> compressors = new ArrayList<>();

        ObjectInputStream ois = null;
        try {
            FileStatus fileStatus[] = fs.listStatus(directory);
            for (FileStatus status : fileStatus) {
                FSDataInputStream inputStream = fs.open(status.getPath());
                ois = new ObjectInputStream(inputStream);
                WrappedGorillaCompressor copyCompressor = (WrappedGorillaCompressor) ois.readObject();
                compressors.add(copyCompressor);
            }
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        } finally {
            if (ois != null) {
                ois.close();
            }
        }
        return compressors;
    }

    public void archiveCurrentCompressor() {
        synchronized (this) {
            if (current != null) {
                current.close();
                archivedCompressors.add(current);
                current = null;
            }
        }
    }

    public List<WrappedGorillaDecompressor> getDecompressors(long begin, long end) {

        List<WrappedGorillaDecompressor> decompressors = new ArrayList<>();

        synchronized (archivedCompressors) {
            for (WrappedGorillaCompressor r : archivedCompressors) {
                if (r.inRange(begin, end)) {
                    GorillaDecompressor d = new GorillaDecompressor(new LongArrayInput(r.getCompressorOutput()));
                    // use -1 length since this compressor is closed
                    decompressors.add(new WrappedGorillaDecompressor(d, -1));
                }
            }
        }

        // as long as begin is inRange, we should use the data in the current
        // compressor as well
        synchronized (this) {
            if (current != null && current.inRange(begin, end)) {
                LongArrayInput decompressorByteBufferInput = new LongArrayInput(current.getCompressorOutput());
                GorillaDecompressor d = new GorillaDecompressor(decompressorByteBufferInput);

                WrappedGorillaDecompressor wd = new WrappedGorillaDecompressor(d, current.getNumEntries());
                System.out.println("creating Decompressor " + d + " from Compressor " + wd + " numEntries="
                        + current.getNumEntries());
                decompressors.add(wd);
            }
        }
        return decompressors;
    }

    public void addValue(long timestamp, double value) {

        // values must be inserted in order
        if (timestamp >= newestTimestamp) {
            newestTimestamp = timestamp;
            synchronized (this) {
                getCompressor(timestamp).addValue(timestamp, value);
            }
        }
    }

    public long getNewestTimestamp() {
        return newestTimestamp;
    }

    public long getOldestTimestamp() {
        return oldestTimestamp;
    }
}
