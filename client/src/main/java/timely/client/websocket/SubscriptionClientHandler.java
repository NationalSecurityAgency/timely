package timely.client.websocket;

import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionClientHandler extends Endpoint {

    private final static Logger LOG = LoggerFactory.getLogger(SubscriptionClientHandler.class);

    @Override
    public void onOpen(Session session, EndpointConfig config) {
        LOG.debug("Websocket session {} opened.", session.getId());
        session.addMessageHandler(new MessageHandler.Whole<String>() {

            @Override
            public void onMessage(String message) {
                LOG.debug("Message received on Websocket session {}: {}", session.getId(), message);
            }
        });
    }

    @Override
    public void onClose(Session session, CloseReason reason) {
        LOG.debug("Websocket session {} closed.", session.getId());
    }

    @Override
    public void onError(Session session, Throwable error) {
        LOG.error("Error occurred on Websocket session" + session.getId(), error);
    }

}
