package timely.auth;

import java.security.cert.X509Certificate;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.web.authentication.preauth.x509.SubjectDnX509PrincipalExtractor;

public class AuthenticationService {

    private static final ApplicationContext springContext = new ClassPathXmlApplicationContext("security.xml");
    private static final AuthenticationManager authManager = (AuthenticationManager) springContext
            .getBean("authenticationManager");
    private static final SubjectDnX509PrincipalExtractor x509 = (SubjectDnX509PrincipalExtractor) springContext
            .getBean("x509PrincipalExtractor");

    public static AuthenticationManager getAuthenticationManager() {
        return authManager;
    }

    public static String extractDN(X509Certificate clientCert) throws Exception {
        return (String) x509.extractPrincipal(clientCert);
    }

}
