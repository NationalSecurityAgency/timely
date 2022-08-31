package timely.server.sample.iterators;

import java.util.LinkedList;

import timely.model.ObjectSizeOf;

public class DownsampleMemoryEstimator {

    private boolean newBucket = false;
    private long start;
    private long period;
    private long startOfCurrentBucket;
    private long bucketsCompleted = 0;
    private long bytesPerBucket = 0;
    boolean highVolumeBuckets = false;
    long maxDownsampleMemory = 0; // max aggregation memory (bytes) before
                                  // current batch is returned (after bucket is
                                  // complete)
    LinkedList<Long> percentageChecks = new LinkedList<>();

    public void reset() {
        newBucket = false;
        bucketsCompleted = 0;
        bytesPerBucket = 0;
        percentageChecks.clear();
        percentageChecks.add(1l);
        percentageChecks.add(5l);
        percentageChecks.add(25l);
        percentageChecks.add(50l);
        percentageChecks.add(75l);
        percentageChecks.add(95l);
    }

    public DownsampleMemoryEstimator(long maxDownsampleMemory, long start, long period) {
        this.maxDownsampleMemory = maxDownsampleMemory;
        this.start = start;
        this.period = period;
        this.startOfCurrentBucket = this.start;
        reset();
    }

    public double getMemoryUsedPercentage() {
        return (bucketsCompleted * bytesPerBucket) / (double) maxDownsampleMemory * 100;
    }

    public long getBucketsCompleted() {
        return bucketsCompleted;
    }

    public long getBytesPerBucket() {
        return bytesPerBucket;
    }

    public boolean isHighVolumeBuckets() {
        return highVolumeBuckets;
    }

    public boolean shouldReturnBasedOnMemoryUsage(long timestamp, Object value) {

        boolean shouldReturn = false;
        if (maxDownsampleMemory >= 0) {
            sample(timestamp);
            if (isNewBucket()) {
                bucketsCompleted++;
                double memoryUsedPercentage = getMemoryUsedPercentage();
                if (memoryUsedPercentage >= 100) {
                    shouldReturn = true;
                } else {
                    boolean checkMemoryNow = false;
                    Long check = percentageChecks.peek();
                    if (check != null && memoryUsedPercentage >= check) {
                        checkMemoryNow = true;
                        percentageChecks.removeFirst();
                    }
                    // recalculate bytesPerBucket
                    if (bytesPerBucket < 100 || bucketsCompleted == 10 || highVolumeBuckets || checkMemoryNow) {
                        long memoryUsed = ObjectSizeOf.Sizer.getObjectSize(value);
                        bytesPerBucket = memoryUsed / bucketsCompleted;
                    }
                    // bucket average greater than 10% of max
                    highVolumeBuckets = (bytesPerBucket / (double) maxDownsampleMemory) >= 0.1;
                    shouldReturn = false;
                }
            }
        }
        return shouldReturn;
    }

    public boolean isNewBucket() {
        return newBucket;
    }

    private void sample(long timestamp) {
        if (timestamp >= (startOfCurrentBucket + period)) {
            newBucket = true;
            startOfCurrentBucket = timestamp - ((timestamp - start) % period);
        } else {
            newBucket = false;
        }
    }
}
