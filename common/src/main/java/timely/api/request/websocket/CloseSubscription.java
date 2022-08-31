package timely.api.request.websocket;

import timely.api.annotation.WebSocket;

@WebSocket(operation = "close")
public class CloseSubscription extends SubscriptionRequest {}
