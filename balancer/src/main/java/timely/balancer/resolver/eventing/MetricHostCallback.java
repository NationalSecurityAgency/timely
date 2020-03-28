package timely.balancer.resolver.eventing;

public abstract class MetricHostCallback {

    public abstract void onEvent(MetricHostEvent event);

}
