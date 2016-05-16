package timely.auth;

import java.security.cert.X509Certificate;
import java.util.ServiceConfigurationError;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.web.authentication.preauth.x509.SubjectDnX509PrincipalExtractor;

public class AuthenticationService {

    private static ApplicationContext springContext = null;
    private static AuthenticationManager authManager = null;
    private static SubjectDnX509PrincipalExtractor x509 = null;

    static {
        try {
            springContext = new ClassPathXmlApplicationContext("security.xml");
            authManager = (AuthenticationManager) springContext.getBean("authenticationManager");
            x509 = (SubjectDnX509PrincipalExtractor) springContext.getBean("x509PrincipalExtractor");
        } catch (BeansException e) {
            throw new ServiceConfigurationError("Error setting up Authentication objects: " + e.getMessage(),
                    e.getRootCause());
        }
    }

    public static AuthenticationManager getAuthenticationManager() {
        return authManager;
    }

    public static String extractDN(X509Certificate clientCert) throws Exception {
        return (String) x509.extractPrincipal(clientCert);
    }

}
