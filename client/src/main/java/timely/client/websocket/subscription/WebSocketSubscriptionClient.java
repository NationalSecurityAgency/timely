package timely.client.websocket.subscription;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;

import javax.net.ssl.SSLContext;
import javax.websocket.CloseReason;
import javax.websocket.DeploymentException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.AddSubscription;
import timely.api.CloseSubscription;
import timely.api.CreateSubscription;
import timely.api.RemoveSubscription;
import timely.client.websocket.ClientHandler;
import timely.client.websocket.WebSocketClient;
import timely.serialize.JsonSerializer;

public class WebSocketSubscriptionClient extends WebSocketClient {

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketSubscriptionClient.class);
    private final String subscriptionId;

    public WebSocketSubscriptionClient(SSLContext ssl, String timelyHostname, int timelyHttpsPort, int timelyWssPort,
            boolean doLogin, String timelyUsername, String timelyPassword, boolean hostVerificationEnabled,
            int bufferSize) {
        super(ssl, timelyHostname, timelyHttpsPort, timelyWssPort, doLogin, timelyUsername, timelyPassword,
                hostVerificationEnabled, bufferSize);
        subscriptionId = UUID.randomUUID().toString();
        LOG.trace("Created WebSocketClient with subscriptionId {}", this.subscriptionId);
    }

    public WebSocketSubscriptionClient(String timelyHostname, int timelyHttpsPort, int timelyWssPort, boolean doLogin,
            String timelyUsername, String timelyPassword, String keyStoreFile, String keyStoreType,
            String keyStorePass, String trustStoreFile, String trustStoreType, String trustStorePass,
            boolean hostVerificationEnabled, int bufferSize) {
        super(timelyHostname, timelyHttpsPort, timelyWssPort, doLogin, timelyUsername, timelyPassword, keyStoreFile,
                keyStoreType, keyStorePass, trustStoreFile, trustStoreType, trustStorePass, hostVerificationEnabled,
                bufferSize);
        subscriptionId = UUID.randomUUID().toString();
        LOG.trace("Created WebSocketClient with subscriptionId {}", this.subscriptionId);
    }

    @Override
    public void open(ClientHandler clientEndpoint) throws IOException, DeploymentException, URISyntaxException {
        super.open(clientEndpoint);
        CreateSubscription create = new CreateSubscription();
        create.setSubscriptionId(subscriptionId);
        session.getBasicRemote().sendText(JsonSerializer.getObjectMapper().writeValueAsString(create));
    }

    @Override
    public synchronized void close() throws IOException {
        if (null != session) {
            CloseSubscription close = new CloseSubscription();
            close.setSubscriptionId(subscriptionId);
            try {
                session.getBasicRemote().sendText(JsonSerializer.getObjectMapper().writeValueAsString(close));
                session.close(new CloseReason(CloseReason.CloseCodes.NORMAL_CLOSURE, "Client called close."));
            } catch (Exception e) {
                LOG.info("Unable to send close message to server: {}", e.getMessage());
            }
        }
        super.close();
    }

    public Future<Void> addSubscription(String metric, Map<String, String> tags, long startTime, long endTime,
            long delayTime) throws IOException {
        AddSubscription add = new AddSubscription();
        add.setSubscriptionId(subscriptionId);
        add.setMetric(metric);
        add.setTags(Optional.ofNullable(tags));
        add.setStartTime(Optional.ofNullable(startTime));
        add.setEndTime(Optional.ofNullable(endTime));
        add.setDelayTime(Optional.ofNullable(delayTime));
        return session.getAsyncRemote().sendText(JsonSerializer.getObjectMapper().writeValueAsString(add));
    }

    public Future<Void> removeSubscription(String metric) throws Exception {
        RemoveSubscription remove = new RemoveSubscription();
        remove.setSubscriptionId(subscriptionId);
        remove.setMetric(metric);
        return session.getAsyncRemote().sendText(JsonSerializer.getObjectMapper().writeValueAsString(remove));
    }

}
