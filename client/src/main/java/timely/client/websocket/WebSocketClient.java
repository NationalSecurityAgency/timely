package timely.client.websocket;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.websocket.DeploymentException;
import javax.websocket.Session;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.glassfish.tyrus.client.ClientManager;
import org.glassfish.tyrus.client.ClientProperties;
import org.glassfish.tyrus.client.SslEngineConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import timely.client.http.HttpClient;

public class WebSocketClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WebSocketClient.class);

    private final String timelyHostname;
    private final int timelyHttpsPort;
    private final int timelyWssPort;
    private final boolean doLogin;
    private final boolean clientAuth;
    private final boolean hostVerificationEnabled;
    private final int bufferSize;
    private final SSLContext ssl;

    private ClientManager webSocketClient = null;
    protected Session session = null;
    protected volatile boolean closed = true;

    protected WebSocketClient(SSLContext ssl, String timelyHostname, int timelyHttpsPort, int timelyWssPort, boolean clientAuth, boolean doLogin,
                    boolean hostVerificationEnabled, int bufferSize) {
        this.ssl = ssl;
        this.timelyHostname = timelyHostname;
        this.timelyHttpsPort = timelyHttpsPort;
        this.timelyWssPort = timelyWssPort;
        this.doLogin = doLogin;
        this.clientAuth = clientAuth;
        this.hostVerificationEnabled = hostVerificationEnabled;
        this.bufferSize = bufferSize;

        Preconditions.checkNotNull(timelyHostname, "%s must be supplied", "Timely host name");
        Preconditions.checkNotNull(timelyHttpsPort, "%s must be supplied", "Timely HTTPS port");
        Preconditions.checkNotNull(timelyWssPort, "%s must be supplied", "Timely WSS port");
    }

    protected WebSocketClient(String timelyHostname, int timelyHttpsPort, int timelyWssPort, boolean clientAuth, boolean doLogin, String keyStoreFile,
                    String keyStoreType, String keyStorePass, String trustStoreFile, String trustStoreType, String trustStorePass,
                    boolean hostVerificationEnabled, int bufferSize) {
        this(HttpClient.getSSLContext(trustStoreFile, trustStoreType, trustStorePass, keyStoreFile, keyStoreType, keyStorePass), timelyHostname,
                        timelyHttpsPort, timelyWssPort, clientAuth, doLogin, hostVerificationEnabled, bufferSize);
    }

    public void open(ClientHandler clientEndpoint) throws IOException, DeploymentException, URISyntaxException {
        Cookie sessionCookie = null;
        if (doLogin) {
            BasicCookieStore cookieJar = new BasicCookieStore();
            try (CloseableHttpClient client = HttpClient.get(ssl, cookieJar, hostVerificationEnabled, clientAuth)) {

                String target = "https://" + timelyHostname + ":" + timelyHttpsPort + "/login";
                HttpRequestBase request = null;
                // HTTP GET to /login to use certificate based login
                request = new HttpGet(target);
                log.trace("Performing client certificate login");

                HttpResponse response = null;
                try {
                    response = client.execute(request);
                    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                        throw new HttpResponseException(response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
                    }
                    for (Cookie c : cookieJar.getCookies()) {
                        if (c.getName().equals("TSESSIONID")) {
                            sessionCookie = c;
                            break;
                        }
                    }
                    if (null == sessionCookie) {
                        throw new IllegalStateException("Unable to find TSESSIONID cookie header in Timely login response");
                    }
                } finally {
                    HttpClientUtils.closeQuietly(response);
                }
            }
        }

        SslEngineConfigurator sslEngine = new SslEngineConfigurator(ssl);
        sslEngine.setClientMode(true);
        sslEngine.setHostVerificationEnabled(hostVerificationEnabled);

        webSocketClient = ClientManager.createClient();
        webSocketClient.getProperties().put(ClientProperties.SSL_ENGINE_CONFIGURATOR, sslEngine);
        webSocketClient.getProperties().put(ClientProperties.INCOMING_BUFFER_SIZE, bufferSize);
        String wssPath = "wss://" + timelyHostname + ":" + timelyWssPort + "/websocket";
        session = webSocketClient.connectToServer(clientEndpoint, new TimelyEndpointConfig(clientEndpoint, sessionCookie), new URI(wssPath));

        final ByteBuffer pingData = ByteBuffer.allocate(0);
        webSocketClient.getScheduledExecutorService().scheduleAtFixedRate(() -> {
            try {
                session.getBasicRemote().sendPing(pingData);
            } catch (Exception e) {
                log.error("Error sending ping", e);
            }
        }, 30, 60, TimeUnit.SECONDS);
        closed = false;
    }

    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            if (null != webSocketClient) {
                webSocketClient.shutdown();
            }
        } finally {
            session = null;
            webSocketClient = null;
            closed = true;
        }
    }

    public boolean isClosed() {
        return closed;
    }

}
