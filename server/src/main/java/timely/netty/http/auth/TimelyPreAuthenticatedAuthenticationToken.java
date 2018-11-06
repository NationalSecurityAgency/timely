package timely.netty.http.auth;

import java.security.cert.X509Certificate;

import io.netty.handler.codec.http.HttpHeaders;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

public class TimelyPreAuthenticatedAuthenticationToken extends PreAuthenticatedAuthenticationToken {

    private HttpHeaders httpHeaders = null;

    public TimelyPreAuthenticatedAuthenticationToken(String subjectDN, X509Certificate clientCert,
            HttpHeaders httpHeaders) {
        super(subjectDN, clientCert);
        this.httpHeaders = httpHeaders;
    }

    public HttpHeaders getHttpHeaders() {
        return httpHeaders;
    }
}
