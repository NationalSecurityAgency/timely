package timely.balancer.resolver.eventing;

import timely.balancer.resolver.BalancedMetricResolver;

public class MetricBalanceEvent {

    public enum ProgressType {
        BEGIN, BALANCER_LOCK_ACQUIRED, END
    }

    private final ProgressType progressType;
    private final BalancedMetricResolver.BalanceType balanceType;
    private final Long numRessigned;

    public MetricBalanceEvent(ProgressType progressType, BalancedMetricResolver.BalanceType balanceType, long numReassigned) {
        this.progressType = progressType;
        this.balanceType = balanceType;
        this.numRessigned = numReassigned;
    }

    public BalancedMetricResolver.BalanceType getBalanceType() {
        return balanceType;
    }

    public ProgressType getProgressType() {
        return progressType;
    }

    public Long getNumRessigned() {
        return numRessigned;
    }
}
