package timely.subscription;

import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionRegistry extends ConcurrentHashMap<String,Subscription> {

    private static final long serialVersionUID = 1L;
    private static final SubscriptionRegistry REGISTRY = new SubscriptionRegistry();

    private SubscriptionRegistry() {}

    public static SubscriptionRegistry get() {
        return REGISTRY;
    }

}
