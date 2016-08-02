package timely.api.request.subscription;

import timely.api.annotation.WebSocket;

@WebSocket(operation = "create")
public class CreateSubscription extends SubscriptionRequest {
}
