package timely.api.request.websocket;

import timely.api.annotation.WebSocket;

@WebSocket(operation = "create")
public class CreateSubscription extends SubscriptionRequest {}
