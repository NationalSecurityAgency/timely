package timely.server.store.cache;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TimeZone;
import java.util.concurrent.locks.StampedLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.LongArrayInput;
import timely.model.Metric;

public final class GorillaStore {

    private static final Logger log = LoggerFactory.getLogger(GorillaStore.class);
    private Queue<WrappedGorillaCompressor> archivedCompressors = new LinkedList<WrappedGorillaCompressor>();
    private StampedLock archivedCompressorLock = new StampedLock();
    private StampedLock currentCompressorLock = new StampedLock();

    transient private WrappedGorillaCompressor current = null;
    transient private List<Metric> metricCache = new ArrayList<>();

    private long oldestTimestamp = Long.MAX_VALUE;
    private long newestTimestamp = -1;
    private long maxAge;
    private String metric;

    public GorillaStore(String metric, long maxAge) {
        this.metric = metric;
        this.maxAge = maxAge;
    }

    public GorillaStore(FileSystem fs, String metric, long maxAge) throws IOException {
        this.metric = metric;
        this.maxAge = maxAge;
        String baseDir = "/timely/cache";
        Path directory = new Path(baseDir + "/" + metric);
        List<WrappedGorillaCompressor> compressors = readCompressors(fs, directory);
        long stamp = archivedCompressorLock.writeLock();
        try {
            archivedCompressors.addAll(compressors);
        } finally {
            archivedCompressorLock.unlockWrite(stamp);
        }
    }

    private WrappedGorillaCompressor getCompressor(long timestamp, long lockStamp) {
        if (current == null) {
            if (oldestTimestamp == Long.MAX_VALUE) {
                oldestTimestamp = timestamp;
            }
            newestTimestamp = timestamp;
            long stamp = lockStamp == 0 ? currentCompressorLock.writeLock() : lockStamp;
            try {
                current = new WrappedGorillaCompressor(timestamp);
            } finally {
                if (lockStamp == 0) {
                    currentCompressorLock.unlockWrite(stamp);
                }
            }
        }
        return current;
    }

    public boolean isEmpty() {
        int compressors;
        long stamp = archivedCompressorLock.readLock();
        try {
            compressors = archivedCompressors.size();
        } finally {
            archivedCompressorLock.unlockRead(stamp);
        }
        stamp = currentCompressorLock.readLock();
        try {
            if (current != null) {
                compressors++;
            }
        } finally {
            currentCompressorLock.unlockRead(stamp);
        }
        return compressors == 0;
    }

    public long ageOffArchivedCompressors() {
        return ageOffArchivedCompressors(maxAge, System.currentTimeMillis());
    }

    public long ageOffArchivedCompressors(long maxAge, long now) {
        long numRemoved = 0;
        long oldestRemainingTimestamp = Long.MAX_VALUE;
        long stamp = archivedCompressorLock.writeLock();
        try {
            if (archivedCompressors.size() > 0) {
                Iterator<WrappedGorillaCompressor> itr = archivedCompressors.iterator();
                while (itr.hasNext()) {
                    WrappedGorillaCompressor c = itr.next();
                    long timeSinceNewestTimestamp = now - c.getNewestTimestamp();
                    if (timeSinceNewestTimestamp >= maxAge) {
                        log.trace("removing archive for {} maxAgeMin:{} oldestInMin:{} youngestInMin:{}", metric, maxAge / (1000 * 60),
                                        (now - c.getOldestTimestamp()) / (1000 * 60), (now - c.getNewestTimestamp()) / (1000 * 60));
                        itr.remove();
                        numRemoved++;
                    } else {
                        log.trace("keeping archive for {} maxAgeMin:{} oldestInMin:{} youngestInMin:{}", metric, maxAge / (1000 * 60),
                                        (now - c.getOldestTimestamp()) / (1000 * 60), (now - c.getNewestTimestamp()) / (1000 * 60));
                        if (c.getOldestTimestamp() < oldestRemainingTimestamp) {
                            if (oldestRemainingTimestamp == Long.MAX_VALUE) {
                                log.trace("changing {} oldestInMin from Long.MAX_VALUE to {}", metric, (now - c.getOldestTimestamp()) / (1000 * 60));
                            } else {
                                log.trace("changing {} oldestInMin from {} to {}", metric, (now - oldestRemainingTimestamp) / (1000 * 60),
                                                (now - c.getOldestTimestamp()) / (1000 * 60));
                            }
                            oldestRemainingTimestamp = c.getOldestTimestamp();
                        }
                    }
                }
            }
        } finally {
            archivedCompressorLock.unlockWrite(stamp);
        }

        if (oldestRemainingTimestamp == Long.MAX_VALUE) {
            stamp = currentCompressorLock.readLock();
            try {
                if (current != null) {
                    log.trace("changing {} oldestInMin from Long.MAX_VALUE to {} (no archives, using current)", metric,
                                    (now - current.getOldestTimestamp()) / (1000 * 60));
                    oldestRemainingTimestamp = current.getOldestTimestamp();
                }
            } finally {
                currentCompressorLock.unlockRead(stamp);
            }
        }

        if (oldestRemainingTimestamp < Long.MAX_VALUE) {
            oldestTimestamp = oldestRemainingTimestamp;
            log.trace("Setting oldestTimestamp for {} to {} ageInMin:{}", metric, oldestTimestamp, (now - oldestTimestamp) / (1000 * 60));
        } else {
            // reset oldestTimestamp to Long.MAX_VALUE since we don't have any of that
            // metric left
            oldestTimestamp = Long.MAX_VALUE;
            log.trace("No archives and no current for {}", metric);
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

        long archiveStamp = archivedCompressorLock.writeLock();
        long currentStamp = currentCompressorLock.writeLock();
        try {
            if (current != null) {
                current.close();
                archivedCompressors.add(current);
                current = null;
            }
        } finally {
            archivedCompressorLock.unlockWrite(archiveStamp);
            currentCompressorLock.unlockWrite(currentStamp);
        }
    }

    public List<WrappedGorillaDecompressor> getDecompressors(long begin, long end) {

        List<WrappedGorillaDecompressor> decompressors = new ArrayList<>();

        long stamp = archivedCompressorLock.readLock();
        try {
            for (WrappedGorillaCompressor r : archivedCompressors) {
                if (r.inRange(begin, end)) {
                    GorillaDecompressor d = new GorillaDecompressor(new LongArrayInput(r.getCompressorOutput()));
                    // use -1 length since this compressor is closed
                    decompressors.add(new WrappedGorillaDecompressor(d, -1));
                }
            }
        } finally {
            archivedCompressorLock.unlockRead(stamp);
        }

        // as long as begin is inRange, we should use the data in the current
        // compressor as well
        stamp = currentCompressorLock.readLock();
        try {
            if (current != null && current.inRange(begin, end)) {
                LongArrayInput decompressorByteBufferInput = new LongArrayInput(current.getCompressorOutput());
                GorillaDecompressor d = new GorillaDecompressor(decompressorByteBufferInput);

                WrappedGorillaDecompressor wd = new WrappedGorillaDecompressor(d, current.getNumEntries());
                decompressors.add(wd);
            }
        } finally {
            currentCompressorLock.unlockRead(stamp);
        }
        return decompressors;
    }

    public void flush() {

        List<Metric> tempCache = new ArrayList<>();
        synchronized (metricCache) {
            tempCache.addAll(metricCache);
            metricCache.clear();
        }

        long stamp = currentCompressorLock.writeLock();
        try {
            WrappedGorillaCompressor c = null;
            long now = System.currentTimeMillis();
            for (Metric m : tempCache) {
                long ts = m.getValue().getTimestamp();
                double v = m.getValue().getMeasure();
                if (ts > newestTimestamp && (now - ts) < maxAge) {
                    newestTimestamp = ts;
                    if (c == null) {
                        c = getCompressor(ts, stamp);
                    }
                    c.addValue(ts, v);
                }
            }
        } finally {
            currentCompressorLock.unlockWrite(stamp);
        }
    }

    public void addValue(Metric metric) {
        long now = System.currentTimeMillis();
        long timestamp = metric.getValue().getTimestamp();
        if (timestamp >= newestTimestamp && (now - timestamp) < maxAge) {
            synchronized (metricCache) {
                metricCache.add(metric);
            }
        }
    }

    public void addValue(long timestamp, double value) {

        long now = System.currentTimeMillis();
        // values must be inserted in order
        if (timestamp >= newestTimestamp && (now - timestamp) < maxAge) {
            newestTimestamp = timestamp;
            long stamp = currentCompressorLock.writeLock();
            try {
                getCompressor(timestamp, stamp).addValue(timestamp, value);
            } finally {
                currentCompressorLock.unlockWrite(stamp);
            }
        }
    }

    public long getNewestTimestamp() {
        return newestTimestamp;
    }

    public long getOldestTimestamp() {
        return oldestTimestamp;
    }

    public long getNumEntries() {

        long numEntries = 0;
        long stamp = currentCompressorLock.readLock();
        try {
            if (current != null) {
                numEntries += current.getNumEntries();
            }
        } finally {
            currentCompressorLock.unlockRead(stamp);
        }
        stamp = archivedCompressorLock.readLock();
        try {
            if (archivedCompressors.size() > 0) {
                Iterator<WrappedGorillaCompressor> itr = archivedCompressors.iterator();
                while (itr.hasNext()) {
                    WrappedGorillaCompressor c = itr.next();
                    numEntries += c.getNumEntries();
                }
            }
        } finally {
            archivedCompressorLock.unlockRead(stamp);
        }
        return numEntries;
    }
}
