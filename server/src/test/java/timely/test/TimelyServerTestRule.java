package timely.test;

import org.apache.accumulo.core.client.AccumuloClient;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.common.configuration.TimelyProperties;
import timely.util.TestUtils;

public class TimelyServerTestRule extends TestWatcher {

    private static final Logger log = LoggerFactory.getLogger(TimelyServerTestRule.class);
    private TimelyProperties timelyProperties;
    private AccumuloClient accumuloClient;

    public TimelyServerTestRule(TimelyProperties timelyProperties, AccumuloClient accumuloClient) {
        this.timelyProperties = timelyProperties;
        this.accumuloClient = accumuloClient;
    }

    @Override
    protected void failed(Throwable e, Description description) {
        log.error("Test {}:{} failed", description.getTestClass().getCanonicalName(), description.getMethodName());
        log.error(e.getMessage(), e);
        if (accumuloClient != null) {
            if (accumuloClient.tableOperations().exists(timelyProperties.getMetricsTable())) {
                separator();
                log.info("Contents of {}", timelyProperties.getMetricsTable());
                separator();
                TestUtils.printTable(accumuloClient, timelyProperties.getMetricsTable(), true);
                separator();
            }
            if (accumuloClient.tableOperations().exists(timelyProperties.getMetaTable())) {
                log.info("Contents of {}", timelyProperties.getMetaTable());
                separator();
                TestUtils.printTable(accumuloClient, timelyProperties.getMetaTable(), false);
                separator();
            }
        }
    }

    private void separator() {
        log.info("-----------------------------------------------------------------------------------");
    }

    @Override
    protected void starting(Description description) {
        separator();
        log.info("Starting test {}:{}", description.getTestClass().getCanonicalName(), description.getMethodName());
        separator();
    }

    @Override
    protected void finished(Description description) {
        separator();
        log.info("Finished test {}:{}", description.getTestClass().getCanonicalName(), description.getMethodName());
        separator();
    }
}
