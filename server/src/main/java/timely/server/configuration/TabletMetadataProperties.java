package timely.server.configuration;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

import timely.server.store.MetaAgeOffIterator;

@ConfigurationProperties(prefix = "timely.tablet")
public class TabletMetadataProperties {
    private static final Logger log = LoggerFactory.getLogger(MetaAgeOffIterator.class);
    private TimeUnit timeUnit = TimeUnit.DAYS;
    private boolean disableTabletRowCheckFilter = false;

    public void setTimeUnit(String timeUnit) {
        try {
            this.timeUnit = TimeUnit.valueOf(timeUnit.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.error(e.getMessage(), e);
        }
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setDisableTabletRowCheckFilter(boolean disableTabletRowCheckFilter) {
        this.disableTabletRowCheckFilter = disableTabletRowCheckFilter;
    }

    public boolean getDisableTabletRowCheckFilter() {
        return disableTabletRowCheckFilter;
    }
}
