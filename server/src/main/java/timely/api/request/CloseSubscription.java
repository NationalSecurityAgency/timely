package timely.api.request;

import timely.api.annotation.WebSocket;

@WebSocket(operation = "close")
public class CloseSubscription extends WebSocketRequest {
}
