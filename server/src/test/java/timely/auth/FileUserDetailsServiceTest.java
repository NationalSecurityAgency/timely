package timely.auth;

import java.util.Arrays;
import java.util.ServiceConfigurationError;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class FileUserDetailsServiceTest {

    private static ApplicationContext springContext = null;
    private static FileUserDetailsService fileUserDetailsService = null;

    @BeforeClass
    public static void setupClass() {
        try {
            springContext = new ClassPathXmlApplicationContext("security.xml");
            fileUserDetailsService = (FileUserDetailsService) springContext.getBean("timelyUserDetailsService");
        } catch (BeansException e) {
            throw new ServiceConfigurationError("Error initializing from spring: " + e.getMessage(), e.getRootCause());
        }
    }

    @Test
    public void testAuths() {
        TimelyUser u = fileUserDetailsService.getUsers().get("CN=example.com");
        Assert.assertNotNull("TimelyUser not found", u);
        Assert.assertEquals("3 auths expected", 3, u.getAuths().size());
        Assert.assertTrue("Unexpected auths found " + u.getAuths(), u.getAuths().containsAll(Arrays.asList("D", "E", "F")));
    }

    @Test
    public void testRoles() {
        TimelyUser u = fileUserDetailsService.getUsers().get("CN=example.com");
        Assert.assertNotNull("TimelyUser not found", u);
        Assert.assertEquals("3 roles expected", 3, u.getRoles().size());
        Assert.assertTrue("Unexpected roles found " + u.getRoles(), u.getRoles().containsAll(Arrays.asList("G", "H", "I")));
    }
}
