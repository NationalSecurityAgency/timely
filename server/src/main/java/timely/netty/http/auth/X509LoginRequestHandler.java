package timely.netty.http.auth;

import java.security.cert.X509Certificate;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.SslHandler;
import org.springframework.security.core.Authentication;
import timely.api.request.auth.X509LoginRequest;
import timely.auth.AuthenticationService;
import timely.configuration.Http;
import timely.configuration.Security;

public class X509LoginRequestHandler extends TimelyLoginRequestHandler<X509LoginRequest> {

    public X509LoginRequestHandler(Security security, Http http) {
        super(security, http);
    }

    @Override
    protected Authentication authenticate(ChannelHandlerContext ctx, X509LoginRequest loginRequest) throws Exception {
        // If we are operating in 2 way SSL, then get the subjectDN from the
        // client certificate and perform the login process.
        SslHandler sslHandler = (SslHandler) ctx.channel().pipeline().get("ssl");
        if (null != sslHandler) {
            X509Certificate clientCert = (X509Certificate) sslHandler.engine().getSession().getPeerCertificates()[0];
            String subjectDN = AuthenticationService.extractDN(clientCert);
            HttpHeaders httpHeaders = loginRequest.getHttpRequest().headers();
            TimelyAuthenticationToken token = new TimelyAuthenticationToken(subjectDN,
                    clientCert, httpHeaders);
            return AuthenticationService.getAuthenticationManager().authenticate(token);
        } else {
            throw new IllegalStateException("The expected SSL handler is not in the pipeline.");
        }
    }

}
