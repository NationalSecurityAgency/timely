package timely.test.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;

import java.io.OutputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import timely.Server;
import timely.api.request.auth.BasicAuthLoginRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.timeseries.QueryResponse;
import timely.auth.AuthCache;
import timely.netty.Constants;
import timely.test.IntegrationTest;
import timely.util.JsonUtil;

/**
 *
 * Tests that OneWay SSL without anonymous access works.
 *
 */
@Category(IntegrationTest.class)
public class OneWaySSLBasicAuthAccessIT extends OneWaySSLBase {

    private static final Long TEST_TIME = System.currentTimeMillis();

    protected HttpsURLConnection getUrlConnection(URL url) throws Exception {
        // Username and password are set in src/test/resources/security.xml
        return getUrlConnection("test", "test1", url);
    }

    protected HttpsURLConnection getUrlConnection(String username, String password, URL url) throws Exception {
        HttpsURLConnection.setDefaultSSLSocketFactory(getSSLSocketFactory());
        URL loginURL = new URL(url.getProtocol() + "://" + url.getHost() + ":" + url.getPort() + "/login");
        HttpsURLConnection con = (HttpsURLConnection) loginURL.openConnection();
        con.setHostnameVerifier((host, session) -> true);
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        BasicAuthLoginRequest request = new BasicAuthLoginRequest();
        request.setUsername(username);
        request.setPassword(password);
        String requestJSON = JsonUtil.getObjectMapper().writeValueAsString(request);
        con.setRequestProperty("Content-Length", String.valueOf(requestJSON.length()));
        OutputStream wr = con.getOutputStream();
        wr.write(requestJSON.getBytes(UTF_8));
        int responseCode = con.getResponseCode();
        if (401 == responseCode) {
            throw new UnauthorizedUserException();
        }
        Assert.assertEquals(200, responseCode);
        List<String> cookies = con.getHeaderFields().get(Names.SET_COOKIE);
        Assert.assertEquals(1, cookies.size());
        Cookie sessionCookie = ClientCookieDecoder.STRICT.decode(cookies.get(0));
        Assert.assertEquals(Constants.COOKIE_NAME, sessionCookie.name());
        con = (HttpsURLConnection) url.openConnection();
        con.setRequestProperty(Names.COOKIE, sessionCookie.name() + "=" + sessionCookie.value());
        con.setHostnameVerifier((host, session) -> true);
        return con;
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        OneWaySSLBase.beforeClass();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        OneWaySSLBase.afterClass();
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        Connector con = mac.getConnector("root", "secret");
        con.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B", "C", "D", "E", "F"));
    }

    @After
    public void tearDown() throws Exception {
        AuthCache.resetSessionMaxAge();
    }

    @Test
    public void testBasicAuthLogin() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            String metrics = "https://localhost:54322/api/metrics";
            query(metrics);
        } finally {
            s.shutdown();
        }
    }

    @Test(expected = UnauthorizedUserException.class)
    public void testBasicAuthLoginFailure() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            String metrics = "https://localhost:54322/api/metrics";
            query("test", "test2", metrics);
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testQueryWithVisibility() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1000)
                    + " 3.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 2000)
                    + " 2.0 tag1=value1 tag3=value3 viz=A", "sys.cpu.user " + (TEST_TIME + 3000)
                    + " 2.0 tag1=value1 tag3=value3 viz=D", "sys.cpu.user " + (TEST_TIME + 3000)
                    + " 2.0 tag1=value1 tag3=value3 viz=G");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(4, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
            subQuery.setMetric("sys.cpu.user");
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            String metrics = "https://127.0.0.1:54322/api/query";
            List<QueryResponse> response = query("test", "test1", metrics, request);
            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(0, tags.size());
            Map<String, Object> dps = response.get(0).getDps();
            // test user only has authorities A,B,C. So it does not see D and G.
            assertEquals(3, dps.size());
        } finally {
            s.shutdown();
        }
    }
}
