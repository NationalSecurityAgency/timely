package timely.balancer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.balancer.configuration.BalancerProperties;
import timely.balancer.configuration.BalancerServerProperties;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.tcp.TcpClientPool;
import timely.balancer.healthcheck.TCPServerHealthCheck;

public class HealthChecker {

    private static final Logger log = LoggerFactory.getLogger(HealthChecker.class);
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private TCPServerHealthCheck tcpServerHealthCheck;
    private BalancerProperties balancerProperties;
    private BalancerServerProperties balancerServerProperties;
    protected List<TimelyBalancedHost> timelyHosts = new ArrayList<>();

    public HealthChecker(BalancerProperties balancerProperties, BalancerServerProperties balancerServerProperties) {
        this.balancerProperties = balancerProperties;
        this.balancerServerProperties = balancerServerProperties;
    }

    public void start() {
        TcpClientPool tcpClientPool = new TcpClientPool(balancerServerProperties);
        this.tcpServerHealthCheck = new TCPServerHealthCheck(tcpClientPool);
        this.executorService.scheduleAtFixedRate(() -> {
            synchronized (timelyHosts) {
                for (TimelyBalancedHost h : timelyHosts) {
                    try {
                        if (tcpServerHealthCheck.isServerHealthy(h)) {
                            h.reportSuccess();
                        } else {
                            h.reportFailure();
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                        h.reportFailure();
                    }
                }
            }
        }, 0, balancerProperties.getCheckServerHealthInterval(), TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
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

    public void setTimelyHosts(List<TimelyBalancedHost> timelyHosts) {
        synchronized (timelyHosts) {
            this.timelyHosts.clear();
            this.timelyHosts.addAll(timelyHosts);
        }
    }
}
