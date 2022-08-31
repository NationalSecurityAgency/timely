package timely.netty.websocket.subscription;

import io.netty.util.AttributeKey;

public class SubscriptionConstants {

    public static final AttributeKey<String> SESSION_ID_ATTR = AttributeKey.newInstance("sessionId");
    public static final AttributeKey<String> SUBSCRIPTION_ID_ATTR = AttributeKey.newInstance("subscriptionId");
}
