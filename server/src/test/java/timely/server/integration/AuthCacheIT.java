package timely.server.integration;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyAuthenticationToken;
import timely.common.component.AuthenticationService;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class AuthCacheIT extends ITBase {

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    private AuthenticationService authenticationService;

    private SubjectIssuerDNPair userExists = SubjectIssuerDNPair.of("cn=example.com");
    private String cookie = null;

    @Before
    public void setup() {
        super.setup();
        try {
            authenticationService.getAuthCache().clear();
            cookie = URLEncoder.encode(UUID.randomUUID().toString(), StandardCharsets.UTF_8.name());
            TimelyAuthenticationToken token = new TimelyAuthenticationToken(userExists.subjectDN(), userExists.issuerDN(), new Object());
            authenticationService.authenticate(token, userExists, cookie);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSessionIdNull() {
        authenticationService.getAuthCache().getAuthorizations("");
    }

    @Test
    public void testGetAuths() {
        Collection<Authorizations> auths = authenticationService.getAuthCache().getAuthorizations(cookie);
        Assert.assertEquals(1, auths.size());
        String[] authStrings = auths.stream().findFirst().get().toString().split(",");
        List<String> sortedAuths = Arrays.stream(authStrings).sorted().collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("D", "E", "F", "X", "Y", "Z"), sortedAuths);
    }
}
