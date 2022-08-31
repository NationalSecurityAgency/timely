package timely.server.integration;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.test.context.junit4.SpringRunner;

import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyAuthenticationToken;
import timely.auth.TimelyPrincipal;
import timely.common.component.AuthenticationService;
import timely.common.configuration.SecurityProperties;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class AuthenticationServiceIT extends ITBase {

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private SecurityProperties securityProperties;

    @Before
    public void setup() {
        super.setup();
        authenticationService.getAuthCache().clear();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    private SubjectIssuerDNPair userExists = SubjectIssuerDNPair.of("cn=example.com");
    private SubjectIssuerDNPair userDoesNotExist = SubjectIssuerDNPair.of("cn=bad.example.com");

    @Test
    public void testX509AuthenticationLogin() {
        TimelyAuthenticationToken token = new TimelyAuthenticationToken(userExists.subjectDN(), userExists.issuerDN(), new Object());
        TimelyPrincipal principal = authenticationService.authenticate(token, userExists, "test");
        Collection<? extends Collection<String>> authorizations = principal.getAuthorizations();
        Collection<String> authCollection = authorizations.stream().findFirst().get();
        Assert.assertTrue(authCollection.containsAll(Arrays.asList("D", "E", "F")));
    }

    @Test(expected = UsernameNotFoundException.class)
    public void testX509AuthenticationLoginFailed() {
        TimelyAuthenticationToken token = new TimelyAuthenticationToken(userDoesNotExist.subjectDN(), userDoesNotExist.issuerDN(), new Object());
        authenticationService.authenticate(token, userDoesNotExist, "test");
    }

    @Test
    public void testRequiredRoles() {
        Assert.assertEquals("required roles incorrect", Arrays.asList("G", "H"), securityProperties.getRequiredRoles());
    }

    @Test
    public void testRequiredAuths() {
        Assert.assertEquals("required auths incorrect", Arrays.asList("X", "Y", "Z"), securityProperties.getRequiredAuths());
    }
}
