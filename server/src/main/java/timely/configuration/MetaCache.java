package timely.configuration;

public class MetaCache {

    private long expirationMinutes = 60;
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
