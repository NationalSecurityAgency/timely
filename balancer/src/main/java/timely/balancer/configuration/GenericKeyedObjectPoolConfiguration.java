package timely.balancer.configuration;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

public class GenericKeyedObjectPoolConfiguration<T> extends GenericKeyedObjectPoolConfig<T> {

    private Duration removeAbandonedTimeout = null;
    private boolean removeAbandonedLogging = false;
    private boolean removeAbandonedOnBorrow = false;
    private boolean removeAbandonedOnMaintenance = false;

    public void setMaxWaitMillis(long maxWaitMillis) {
        super.setMaxWait(Duration.ofMillis(maxWaitMillis));
    }

    public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
        super.setMinEvictableIdleTime(Duration.ofMillis(minEvictableIdleTimeMillis));
    }

    public void setEvictorShutdownTimeoutMillis(long evictorShutdownTimeoutMillis) {
        super.setEvictorShutdownTimeout(Duration.ofMillis(evictorShutdownTimeoutMillis));
    }

    public void setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis) {
        super.setSoftMinEvictableIdleTime(Duration.ofMillis(softMinEvictableIdleTimeMillis));
    }

    public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
        super.setTimeBetweenEvictionRuns(Duration.ofMillis(timeBetweenEvictionRunsMillis));
    }

    public void setRemoveAbandonedTimeoutMillis(long removeAbandonedTimeoutMillis) {
        this.removeAbandonedTimeout = Duration.ofMillis(removeAbandonedTimeoutMillis);
    }

    public Duration getRemoveAbandonedTimeout() {
        return removeAbandonedTimeout;
    }

    public void setRemoveAbandonedOnBorrow(boolean removeAbandonedOnBorrow) {
        this.removeAbandonedOnBorrow = removeAbandonedOnBorrow;
    }

    public boolean getRemoveAbandonedOnBorrow() {
        return removeAbandonedOnBorrow;
    }

    public void setRemoveAbandonedOnMaintenance(boolean removeAbandonedOnMaintenance) {
        this.removeAbandonedOnMaintenance = removeAbandonedOnMaintenance;
    }

    public boolean getRemoveAbandonedOnMaintenance() {
        return removeAbandonedOnMaintenance;
    }

    public void setRemoveAbandonedLogging(boolean removeAbandonedLogging) {
        this.removeAbandonedLogging = removeAbandonedLogging;
    }

    public boolean getRemoveAbandonedLogging() {
        return removeAbandonedLogging;
    }

    public AbandonedConfig getAbandonedConfig() {
        AbandonedConfig abandonedConfig = null;
        if (removeAbandonedTimeout != null) {
            abandonedConfig = new AbandonedConfig();
            abandonedConfig.setRemoveAbandonedTimeout(removeAbandonedTimeout);
            if (removeAbandonedLogging) {
                abandonedConfig.setLogAbandoned(removeAbandonedLogging);
                abandonedConfig.setLogWriter(new PrintWriter(System.out, true, StandardCharsets.UTF_8));
            }
            abandonedConfig.setRemoveAbandonedOnBorrow(removeAbandonedOnBorrow);
            abandonedConfig.setRemoveAbandonedOnMaintenance(removeAbandonedOnMaintenance);
        }
        return abandonedConfig;
    }
}
