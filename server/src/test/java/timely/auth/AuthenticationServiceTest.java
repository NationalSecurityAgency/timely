package timely.auth;

import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import timely.configuration.Configuration;
import timely.test.TestConfiguration;

public class AuthenticationServiceTest {

    private static Configuration config;

    @BeforeClass
    public static void before() {
        config = TestConfiguration.createMinimalConfigurationForTest();
        config.getSecurity().setSessionMaxAge(Integer.MAX_VALUE);
        AuthCache.resetSessionMaxAge();
        AuthCache.setSessionMaxAge(config.getSecurity());
    }

    @Before
    public void setup() {

        AuthCache.clear();
    }

    @AfterClass
    public static void after() {
        AuthCache.resetSessionMaxAge();
    }

    @Test(expected = BadCredentialsException.class)
    public void testBasicAuthenticationFailure() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("test", "test2");
        AuthenticationService.authenticate(token, "test");
    }

    @Test
    public void testBasicAuthenticationLogin() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("test", "test1");
        TimelyPrincipal principal = AuthenticationService.authenticate(token, "test");
        Collection<? extends Collection<String>> authorizations = principal.getAuthorizations();
        authorizations.forEach(a -> {
            Assert.assertTrue(a.contains("A") || a.contains("B") || a.contains("C"));
        });
    }

    @Test
    public void testX509AuthenticationLogin() {
        PreAuthenticatedAuthenticationToken token = new PreAuthenticatedAuthenticationToken("CN=example.com",
                "doesn't matter what I put here");
        TimelyPrincipal principal = AuthenticationService.authenticate(token, "test");
        Collection<? extends Collection<String>> authorizations = principal.getAuthorizations();
        authorizations.forEach(a -> {
            Assert.assertTrue(a.contains("D") || a.contains("E") || a.contains("F"));
        });
    }

    @Test(expected = UsernameNotFoundException.class)
    public void testX509AuthenticationLoginFailed() {
        PreAuthenticatedAuthenticationToken token = new PreAuthenticatedAuthenticationToken("CN=bad.example.com",
                "doesn't matter what I put here");
        TimelyPrincipal principal = AuthenticationService.authenticate(token, "test");
        Collection<? extends Collection<String>> authorizations = principal.getAuthorizations();
        authorizations.forEach(a -> {
            Assert.assertTrue(a.contains("D") || a.contains("E") || a.contains("F"));
        });
    }

}
