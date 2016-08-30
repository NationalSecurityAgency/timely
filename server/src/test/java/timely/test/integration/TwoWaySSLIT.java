package timely.test.integration;

import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.JdkSslClientContext;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import timely.Server;
import timely.Configuration;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.timeseries.QueryResponse;
import timely.auth.AuthCache;
import timely.netty.Constants;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

/**
 *
 * Tests that Two way SSL without anonymous access works.
 *
 */
@SuppressWarnings("deprecation")
@Category(IntegrationTest.class)
public class TwoWaySSLIT extends QueryBase {

    private static final Long TEST_TIME = System.currentTimeMillis();

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static MiniAccumuloCluster mac = null;
    private static Configuration conf = null;

    protected static SelfSignedCertificate serverCert = null;
    protected static File clientTrustStoreFile = null;

    static {
        try {
            serverCert = new SelfSignedCertificate();
            clientTrustStoreFile = serverCert.certificate().getAbsoluteFile();
        } catch (Exception e) {
            throw new RuntimeException("Error creating self signed certificate", e);
        }
    }

    protected SSLSocketFactory getSSLSocketFactory() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();
        builder.applicationProtocolConfig(ApplicationProtocolConfig.DISABLED);
        // Use server cert / key on client side.
        builder.keyManager(serverCert.key(), (String) null, serverCert.cert());
        builder.sslProvider(SslProvider.JDK);
        builder.trustManager(clientTrustStoreFile); // Trust the server cert
        SslContext ctx = builder.build();
        Assert.assertEquals(JdkSslClientContext.class, ctx.getClass());
        JdkSslContext jdk = (JdkSslContext) ctx;
        SSLContext jdkSslContext = jdk.context();
        return jdkSslContext.getSocketFactory();
    }

    protected static void setupSSL(Configuration config) throws Exception {
        config.getSecurity().getSsl().setCertificateFile(serverCert.certificate().getAbsolutePath());
        config.getSecurity().getSsl().setKeyFile(serverCert.privateKey().getAbsolutePath());
        // Needed for 2way SSL
        config.getSecurity().getSsl().setTrustStoreFile(serverCert.certificate().getAbsolutePath());
        config.getSecurity().getSsl().setUseOpenssl(false);
        config.getSecurity().getSsl().setUseGeneratedKeypair(false);
        config.getSecurity().setAllowAnonymousAccess(false);
    }

    protected HttpsURLConnection getUrlConnection(URL url) throws Exception {
        // Username and password not used in 2way SSL case
        return getUrlConnection(null, null, url);
    }

    protected HttpsURLConnection getUrlConnection(String username, String password, URL url) throws Exception {
        HttpsURLConnection.setDefaultSSLSocketFactory(getSSLSocketFactory());
        URL loginURL = new URL(url.getProtocol() + "://" + url.getHost() + ":" + url.getPort() + "/login");
        HttpsURLConnection con = (HttpsURLConnection) loginURL.openConnection();
        con.setHostnameVerifier((host, session) -> true);
        con.setRequestMethod("GET");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        con.connect();
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
        final MiniAccumuloConfig macConfig = new MiniAccumuloConfig(temp.newFolder("mac"), "secret");
        mac = new MiniAccumuloCluster(macConfig);
        mac.start();
        conf = TestConfiguration.createMinimalConfigurationForTest();
        conf.getAccumulo().setInstanceName(mac.getInstanceName());
        conf.getAccumulo().setZookeepers(mac.getZooKeepers());
        setupSSL(conf);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        mac.stop();
    }

    @Before
    public void setup() throws Exception {
        Connector con = mac.getConnector("root", "secret");
        con.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B", "C", "D", "E", "F"));
        con.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    con.tableOperations().delete(t);
                } catch (Exception e) {
                }
            }
        });
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
            sleepUninterruptibly(4, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
            subQuery.setMetric("sys.cpu.user");
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            String metrics = "https://127.0.0.1:54322/api/query";
            List<QueryResponse> response = query(metrics, request);
            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(0, tags.size());
            Map<String, Object> dps = response.get(0).getDps();
            assertEquals(3, dps.size());
        } finally {
            s.shutdown();
        }
    }

}
