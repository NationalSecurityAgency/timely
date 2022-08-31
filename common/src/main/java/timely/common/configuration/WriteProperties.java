package timely.common.configuration;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

@RefreshScope
@ConfigurationProperties(prefix = "timely.accumulo.write")
public class WriteProperties {

    private String latency = "5s";
    private int threads;
    private String bufferSize;

    public WriteProperties() {
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
