package timely.configuration;

public class Scan {

    private int threads = 4;
    private long maxDownsampleMemory = -1;

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public long getMaxDownsampleMemory() {
        return maxDownsampleMemory;
    }

    public void setMaxDownsampleMemory(long maxDownsampleMemory) {
        this.maxDownsampleMemory = maxDownsampleMemory;
    }
}
