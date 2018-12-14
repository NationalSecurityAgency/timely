package timely.configuration;

import org.apache.accumulo.core.client.BatchWriterConfig;

public class Write {

    private String latency = "5s";
    private int threads;
    private String bufferSize;

    public Write() {
        BatchWriterConfig config = new BatchWriterConfig();
        threads = config.getMaxWriteThreads();
        bufferSize = Long.toString(config.getMaxMemory());
    }

    public String getLatency() {
        return latency;
    }

    public void setLatency(String latency) {
        this.latency = latency;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public String getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
    }
}
