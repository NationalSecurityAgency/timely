package timely.balancer.resolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.healthcheck.HealthChecker;
import timely.balancer.healthcheck.TcpHealthChecker;

public class TestHealthChecker implements HealthChecker {

    private final Logger LOG = LoggerFactory.getLogger(TcpHealthChecker.class);
    private List<TimelyBalancedHost> timelyHosts = new ArrayList<>();
    private Map<TimelyBalancedHost, Boolean> serverUpMap = Collections.synchronizedMap(new HashMap<>());
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    public TestHealthChecker(BalancerConfiguration config, List<TimelyBalancedHost> timelyHosts) {
        setTimelyHosts(timelyHosts);
        this.executorService.scheduleAtFixedRate(() -> {
            synchronized (timelyHosts) {
                for (TimelyBalancedHost h : timelyHosts) {
                    try {
                        if (TestHealthChecker.this.isServerHealthy(h)) {
                            h.reportSuccess();
                        } else {
                            h.reportFailure();
                        }
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                        h.reportFailure();
                    }
                }
            }
        }, 0, config.getCheckServerHealthInterval(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void setTimelyHosts(List<TimelyBalancedHost> timelyHosts) {
        synchronized (timelyHosts) {
            this.timelyHosts.clear();
            this.timelyHosts.addAll(timelyHosts);
            for (TimelyBalancedHost h : timelyHosts) {
                serverUpMap.put(h, true);
            }
        }

    }

    public boolean isServerHealthy(TimelyBalancedHost h) {
        return serverUpMap.get(h);
    }

    public void serverUp(TimelyBalancedHost h) {
        serverUpMap.put(h, true);
    }

    public void serverDown(TimelyBalancedHost h) {
        serverUpMap.put(h, false);
    }

    public void serverToggle(TimelyBalancedHost h) {
        synchronized (serverUpMap) {
            serverUpMap.put(h, !serverUpMap.get(h));
        }
    }

    @Override
    public void close() throws Exception {
        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {

        } finally {
            if (!this.executorService.isTerminated()) {
                this.executorService.shutdownNow();
            }
        }
    }
}
