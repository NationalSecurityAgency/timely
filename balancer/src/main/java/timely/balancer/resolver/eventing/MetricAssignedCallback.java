package timely.balancer.resolver.eventing;

public abstract class MetricAssignedCallback {

    public abstract void onEvent(MetricAssignedEvent event);

}
