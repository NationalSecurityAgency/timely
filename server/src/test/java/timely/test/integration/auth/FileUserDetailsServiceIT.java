package timely.test.integration.auth;

import java.util.Arrays;

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

import timely.auth.FileUserDetailsService;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyUser;
import timely.test.IntegrationTest;
import timely.test.TimelyServerTestRule;
import timely.test.integration.ITBase;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class FileUserDetailsServiceIT extends ITBase {

    @Autowired
    @Rule
    public TimelyServerTestRule testRule;

    @Autowired
    private FileUserDetailsService fileUserDetailsService;

    private SubjectIssuerDNPair userExists = SubjectIssuerDNPair.of("cn=example.com");

    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testAuths() {
        TimelyUser u = fileUserDetailsService.getUsers().get(userExists.toString());
        Assert.assertNotNull("TimelyUser not found", u);
        Assert.assertEquals("3 auths expected", 3, u.getAuths().size());
        Assert.assertTrue("Unexpected auths found " + u.getAuths(), u.getAuths().containsAll(Arrays.asList("D", "E", "F")));
    }

    @Test
    public void testRoles() {
        TimelyUser u = fileUserDetailsService.getUsers().get(userExists.toString());
        Assert.assertNotNull("TimelyUser not found", u);
        Assert.assertEquals("3 roles expected", 3, u.getRoles().size());
        Assert.assertTrue("Unexpected roles found " + u.getRoles(), u.getRoles().containsAll(Arrays.asList("G", "H", "I")));
    }
}
