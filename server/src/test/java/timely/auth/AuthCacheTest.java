package timely.auth;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;

import timely.Configuration;
import timely.test.TestConfiguration;

public class AuthCacheTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static Configuration config = null;
    private static String cookie = null;

    @BeforeClass
    public static void before() throws Exception {
        File conf = temp.newFile("config.properties");
        conf.deleteOnExit();
        TestConfiguration cfg = TestConfiguration.createMinimalConfigurationForTest();
        cfg.put(Configuration.SESSION_MAX_AGE, "5000");
        config = cfg.toConfiguration(conf);
        cookie = URLEncoder.encode(UUID.randomUUID().toString(), StandardCharsets.UTF_8.name());
    }

    @Before
    public void setup() throws Exception {
        AuthCache.setSessionMaxAge(config);
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("test", "test1");
        Authentication auth = AuthenticationService.getAuthenticationManager().authenticate(token);
        AuthCache.getCache().put(cookie, auth);
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
        Authorizations a = AuthCache.getAuthorizations(cookie);
        Assert.assertEquals("A,B,C", a.toString());
    }

}
