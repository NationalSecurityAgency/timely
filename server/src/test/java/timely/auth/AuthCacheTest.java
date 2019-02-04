package timely.auth;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.UUID;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import timely.configuration.Configuration;
import timely.netty.http.auth.TimelyAuthenticationToken;
import timely.test.TestConfiguration;

public class AuthCacheTest {

    private static Configuration config;
    private static String cookie = null;

    @BeforeClass
    public static void before() throws Exception {
        config = TestConfiguration.createMinimalConfigurationForTest();
        config.getSecurity().setSessionMaxAge(5000);
        cookie = URLEncoder.encode(UUID.randomUUID().toString(), StandardCharsets.UTF_8.name());
    }

    @Before
    public void setup() throws Exception {
        AuthCache.setSessionMaxAge(config.getSecurity());
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("test", "test1");
        TimelyAuthenticationToken auth = AuthenticationService.authenticate(token);
        AuthCache.getCache().put(cookie, auth.getTimelyPrincipal());
    }

    @After
    public void tearDown() throws Exception {
        AuthCache.resetSessionMaxAge();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSessionIdNull() throws Exception {
        AuthCache.getAuthorizations("");
    }

    @Test
    public void testGetAuths() throws Exception {
        Collection<Authorizations> auths = AuthCache.getAuthorizations(cookie);
        Assert.assertEquals(1, auths.size());
        Authorizations a = auths.iterator().next();
        Assert.assertEquals("A,B,C", a.toString());
    }

}
