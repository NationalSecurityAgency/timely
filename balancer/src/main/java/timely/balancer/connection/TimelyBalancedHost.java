package timely.balancer.connection;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.balancer.ArrivalRate;
import timely.balancer.configuration.BalancerConfiguration;

public class TimelyBalancedHost {

    private static final Logger LOG = LoggerFactory.getLogger(TimelyBalancedHost.class);

    private String host;
    private int tcpPort;
    private int httpPort;
    private int wsPort;
    private int udpPort;
    private boolean isUp = true;
    private BalancerConfiguration balancerConfig;
    private int failures = 0;
    private int successes = 0;
    private ArrivalRate arrivalRate = null;

    private TimelyBalancedHost() {

    }

    private TimelyBalancedHost(String host, int tcpPort, int httpPort, int wsPort, int udpPort,
            ArrivalRate arrivalRate) {
        this.host = host;
        this.tcpPort = tcpPort;
        this.httpPort = httpPort;
        this.wsPort = wsPort;
        this.udpPort = udpPort;
        this.arrivalRate = arrivalRate;
    }

    static public TimelyBalancedHost of(String host, int tcpPort, int httpPort, int wsPort, int udpPort,
            ArrivalRate arrivalRate) {
        return new TimelyBalancedHost(host, tcpPort, httpPort, wsPort, udpPort, arrivalRate);
    }

    static public TimelyBalancedHost of(String host, int basePort) {
        return new TimelyBalancedHost(host, basePort, basePort + 1, basePort + 2, basePort + 3, null);
    }

    public void setBalancerConfig(BalancerConfiguration balancerConfig) {
        this.balancerConfig = balancerConfig;
    }

    synchronized public boolean isUp() {
        return isUp;
    }

    public void reportSuccess() {
        int serverSuccessesBeforeUp = balancerConfig.getServerSuccessesBeforeUp();
        String h = host;
        int p = tcpPort;
        synchronized (this) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("success reported host:{} port:{} isUp:{} successes:{} serverSuccessesBeforeUp:{}", h, p,
                        isUp, (successes + 1), serverSuccessesBeforeUp);
            }
            if (!isUp) {
                if (++successes >= serverSuccessesBeforeUp) {
                    isUp = true;
                    successes = 0;
                    LOG.info("host up host:{} port:{}", h, p);
                }
            }
        }
    }

    public void reportFailure() {
        int serverFailuresBeforeDown = balancerConfig.getServerFailuresBeforeDown();
        String h = host;
        int p = tcpPort;
        synchronized (this) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("failure reported host:{} port:{} isUp:{} failures:{} serverFailuresBeforeUp:{}", h, p, isUp,
                        (failures + 1), serverFailuresBeforeDown);
            }
            if (isUp) {
                if (++failures >= serverFailuresBeforeDown) {
                    isUp = false;
                    failures = 0;
                    LOG.info("host down host:{} port:{}", h, p);
                }
            }
        }
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getUdpPort() {
        return udpPort;
    }

    public void setUdpPort(int udpPort) {
        this.udpPort = udpPort;
    }

    public int getTcpPort() {
        return tcpPort;
    }

    public void setTcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public int getWsPort() {
        return wsPort;
    }

    public void setWsPort(int wsPort) {
        this.wsPort = wsPort;
    }

    public void arrived() {
        if (this.arrivalRate != null) {
            this.arrivalRate.arrived();
        }
    }

    public double getArrivalRate() {
        if (this.arrivalRate == null) {
            return 0;
        } else {
            return this.arrivalRate.getRate();
        }
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(host);
        builder.append(tcpPort);
        builder.append(httpPort);
        builder.append(wsPort);
        builder.append(udpPort);
        return builder.toHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TimelyBalancedHost) {
            TimelyBalancedHost other = (TimelyBalancedHost) o;
            EqualsBuilder builder = new EqualsBuilder();
            builder.append(host, other.host);
            builder.append(tcpPort, other.tcpPort);
            builder.append(httpPort, other.httpPort);
            builder.append(wsPort, other.wsPort);
            builder.append(udpPort, other.udpPort);
            return builder.isEquals();
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append(host);
        builder.append(tcpPort);
        builder.append(httpPort);
        builder.append(wsPort);
        builder.append(udpPort);
        builder.append(isUp);
        return builder.toString();
    }

    public static String toStringShort(TimelyBalancedHost tbh) {
        if (tbh == null) {
            return null;
        }
        return tbh.host + ":" + tbh.tcpPort;
    }
}
