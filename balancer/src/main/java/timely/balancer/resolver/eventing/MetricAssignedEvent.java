package timely.balancer.resolver.eventing;

import timely.balancer.connection.TimelyBalancedHost;

public class MetricAssignedEvent {

    public enum Reason {
        ASSIGN_ROUND_ROBIN, HOST_DOWN_ROUND_ROBIN, ASSIGN_FALLBACK_SEQUENTIAL, HOST_REMOVED, REBALANCE, ASSIGN_FILE
    }

    private final String metric;
    private final TimelyBalancedHost losingHost;
    private final TimelyBalancedHost gainingHost;
    private final Reason reason;

    public MetricAssignedEvent(String metric, TimelyBalancedHost losingHost, TimelyBalancedHost gainingHost,
            Reason reason) {
        this.metric = metric;
        this.losingHost = losingHost;
        this.gainingHost = gainingHost;
        this.reason = reason;
    }

    public String getMetric() {
        return metric;
    }

    public TimelyBalancedHost getLosingHost() {
        return losingHost;
    }

    public TimelyBalancedHost getGainingHost() {
        return gainingHost;
    }

    public Reason getReason() {
        return reason;
    }
}
