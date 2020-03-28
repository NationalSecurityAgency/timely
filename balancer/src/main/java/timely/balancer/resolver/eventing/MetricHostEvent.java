package timely.balancer.resolver.eventing;

import timely.balancer.connection.TimelyBalancedHost;

public class MetricHostEvent {

    public enum ActionType {
        ADDED, REMOVED, SUCCESS, FAILURE
    }

    private final TimelyBalancedHost timelyBalancedHost;
    private final ActionType actionType;

    public MetricHostEvent(TimelyBalancedHost timelyBalancedHost, ActionType actionType) {
        this.timelyBalancedHost = timelyBalancedHost;
        this.actionType = actionType;
    }

    public TimelyBalancedHost getTimelyBalancedHost() {
        return timelyBalancedHost;
    }

    public ActionType getActionType() {
        return actionType;
    }
}
