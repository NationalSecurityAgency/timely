package timely.balancer.healthcheck;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.tcp.TcpClientPool;

public class TcpHealthChecker implements HealthChecker {

    private static final Logger LOG = LoggerFactory.getLogger(TcpHealthChecker.class);
    private List<TimelyBalancedHost> timelyHosts = new ArrayList<>();
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    final private TCPServerHealthCheck tcpServerHealthCheck;

    public TcpHealthChecker(BalancerConfiguration config, TcpClientPool tcpClientPool) {
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
        }
    }

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
