package timely.balancer.resolver.eventing;

public abstract class MetricBalanceCallback {

    public abstract void onEvent(MetricBalanceEvent event);

}
