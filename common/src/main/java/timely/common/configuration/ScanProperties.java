package timely.common.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

@RefreshScope
@ConfigurationProperties(prefix = "timely.accumulo.scan")
public class ScanProperties {

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
