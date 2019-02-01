package timely.netty.http.auth;

import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.AuthenticationException;
import timely.api.request.auth.BasicAuthLoginRequest;
import timely.auth.AuthenticationService;
import timely.auth.TimelyPrincipal;
import timely.configuration.Http;
import timely.configuration.Security;

public class BasicAuthLoginRequestHandler extends TimelyLoginRequestHandler<BasicAuthLoginRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(BasicAuthLoginRequestHandler.class);

    public BasicAuthLoginRequestHandler(Security security, Http http) {
        super(security, http);
    }

    @Override
    protected TimelyPrincipal authenticate(ChannelHandlerContext ctx, BasicAuthLoginRequest msg)
            throws AuthenticationException {
        // Perform the login process using username/password
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(msg.getUsername(),
                msg.getPassword());
        TimelyAuthenticationToken timelyToken = AuthenticationService.authenticate(token);
        TimelyPrincipal principal = timelyToken.getTimelyPrincipal();

        LOG.debug("Authenticated user {} with authorizations {}", principal.getPrimaryUser().getDn().subjectDN(),
                principal.getAuthorizationsString());
        return principal;
    }

}
