package timely.api.response;

import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

public interface WebSocketResponse {

    public TextWebSocketFrame getWebSocketResponse();
}
