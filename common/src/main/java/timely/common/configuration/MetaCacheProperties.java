package timely.common.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

@RefreshScope
@ConfigurationProperties(prefix = "timely.meta-cache")
public class MetaCacheProperties {

    private long expirationMinutes = 1440;
    private int cacheRefreshMinutes = -1;
    private long maxTagValues = 100;

    public long getExpirationMinutes() {
        return expirationMinutes;
    }

    public void setExpirationMinutes(long expirationMinutes) {
        this.expirationMinutes = expirationMinutes;
    }

    public void setCacheRefreshMinutes(int cacheRefreshMinutes) {
        this.cacheRefreshMinutes = cacheRefreshMinutes;
    }

    public int getCacheRefreshMinutes() {
        return cacheRefreshMinutes;
    }

    public long getMaxTagValues() {
        return maxTagValues;
    }

    public void setMaxTagValues(long maxTagValues) {
        this.maxTagValues = maxTagValues;
    }
}
