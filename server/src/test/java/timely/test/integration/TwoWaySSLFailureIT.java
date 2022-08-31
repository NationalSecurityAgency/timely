package timely.test.integration;

import java.net.URL;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import timely.common.component.AuthenticationService;
import timely.common.configuration.HttpProperties;
import timely.common.configuration.SslHelper;
import timely.common.configuration.SslServerProperties;
import timely.netty.Constants;
import timely.test.IntegrationTest;
import timely.test.TimelyServerTestRule;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"twoWaySsl"})
public class TwoWaySSLFailureIT extends TwoWaySSLBase {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    @Autowired
    @Rule
    public TimelyServerTestRule testRule;

    @Autowired
    private HttpProperties httpProperties;

    @Autowired
    private SslServerProperties sslServerProperties;

    @Autowired
    private AuthenticationService authenticationService;

    private SelfSignedCertificate unauthorizedServerCert;
    private String baseUrl;

    @Before
    public void setup() {
        super.setup();
        try {
            // This fqdn is not in security.authorizedUsers
            unauthorizedServerCert = new SelfSignedCertificate("CN=bad.example.com");
        } catch (Exception e) {
            throw new RuntimeException("Error creating self signed certificate", e);
        }
        authenticationService.getAuthCache().clear();
        baseUrl = "https://" + httpProperties.getHost() + ":" + httpProperties.getPort();

    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    protected SSLSocketFactory getSSLSocketFactory() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();
        builder.applicationProtocolConfig(ApplicationProtocolConfig.DISABLED);
        // Use server cert / key on client side
        builder.keyManager(unauthorizedServerCert.key(), (String) null, unauthorizedServerCert.cert());
        builder.sslProvider(SslProvider.JDK);
        builder.trustManager(SslHelper.getTrustManagerFactory(sslServerProperties)); // Trust the server cert
        SslContext ctx = builder.build();
        Assert.assertTrue(ctx.isClient());
        Assert.assertTrue(ctx instanceof JdkSslContext);
        JdkSslContext jdk = (JdkSslContext) ctx;
        SSLContext jdkSslContext = jdk.context();
        return jdkSslContext.getSocketFactory();
    }

    @Override
    protected HttpsURLConnection getUrlConnection(URL url) throws Exception {
        HttpsURLConnection.setDefaultSSLSocketFactory(getSSLSocketFactory());
        URL loginURL = new URL(url.getProtocol() + "://" + url.getHost() + ":" + url.getPort() + "/login");
        HttpsURLConnection con = (HttpsURLConnection) loginURL.openConnection();
        con.setHostnameVerifier((arg0, arg1) -> true);
        con.setRequestMethod("GET");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        con.connect();
        int responseCode = con.getResponseCode();
        if (401 == responseCode) {
            throw new UnauthorizedUserException();
        }
        Assert.assertEquals(200, responseCode);
        List<String> cookies = con.getHeaderFields().get(HttpHeaderNames.SET_COOKIE.toString());
        Assert.assertEquals(1, cookies.size());
        Cookie sessionCookie = ClientCookieDecoder.STRICT.decode(cookies.get(0));
        Assert.assertEquals(Constants.COOKIE_NAME, sessionCookie.name());
        con = (HttpsURLConnection) url.openConnection();
        con.setRequestProperty(HttpHeaderNames.COOKIE.toString(), sessionCookie.name() + "=" + sessionCookie.value());
        con.setHostnameVerifier((arg0, arg1) -> true);
        return con;
    }

    @Test(expected = UnauthorizedUserException.class)
    public void testSslAuthLoginFailure() throws Exception {
        String metrics = baseUrl + "/api/metrics";
        query(metrics);
    }
}
