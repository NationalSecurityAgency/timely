package timely.balancer.healthcheck;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import timely.balancer.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.tcp.TcpClientPool;

public class HealthChecker {

    private List<TimelyBalancedHost> timelyHosts;
    private Timer timer;
    final private TCPServerHealthCheck tcpServerHealthCheck;
    private TimerTask check = new TimerTask() {

        @Override
        public void run() {
            for (TimelyBalancedHost h : timelyHosts) {
                if (tcpServerHealthCheck.isServerHealthy(h)) {
                    h.reportSuccess();
                } else {
                    h.reportFailure();
                }
            }
        }
    };

    public HealthChecker(BalancerConfiguration config, TcpClientPool tcpClientPool) {
        this.timelyHosts = config.getTimelyHosts();
        this.tcpServerHealthCheck = new TCPServerHealthCheck(tcpClientPool);
        this.timer = new Timer("HealthCheckerThread", true);
        this.timer.scheduleAtFixedRate(check, 0, config.getCheckServerHealthInterval());
    }

}
