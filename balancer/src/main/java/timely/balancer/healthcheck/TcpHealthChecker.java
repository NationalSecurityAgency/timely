package timely.balancer.healthcheck;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.tcp.TcpClientPool;

public class TcpHealthChecker implements  HealthChecker {

    private static final Logger LOG = LoggerFactory.getLogger(TcpHealthChecker.class);
    private List<TimelyBalancedHost> timelyHosts = new ArrayList<>();
    private Timer timer;
    final private TCPServerHealthCheck tcpServerHealthCheck;
    private TimerTask check = new TimerTask() {

        @Override
        public void run() {
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
        }
    };

    public TcpHealthChecker(BalancerConfiguration config, TcpClientPool tcpClientPool) {
        this.tcpServerHealthCheck = new TCPServerHealthCheck(tcpClientPool);
        this.timer = new Timer("HealthCheckerThread", true);
        this.timer.scheduleAtFixedRate(check, 0, config.getCheckServerHealthInterval());
    }

    @Override
    public void setTimelyHosts(List<TimelyBalancedHost> timelyHosts) {
        synchronized (timelyHosts) {
            this.timelyHosts.clear();
            this.timelyHosts.addAll(timelyHosts);
        }
    }
}
