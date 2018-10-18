package timely.netty.http.auth;

import io.netty.channel.ChannelHandlerContext;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import timely.Configuration;
import timely.api.request.auth.BasicAuthLoginRequest;
import timely.auth.AuthenticationService;

public class BasicAuthLoginRequestHandler extends TimelyLoginRequestHandler<BasicAuthLoginRequest> {

    public BasicAuthLoginRequestHandler(Configuration conf) {
        super(conf);
    }

    @Override
    protected Authentication authenticate(ChannelHandlerContext ctx, BasicAuthLoginRequest msg)
            throws AuthenticationException {
        // Perform the login process using username/password
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(msg.getUsername(),
                msg.getPassword());
        return AuthenticationService.getAuthenticationManager().authenticate(token);
    }

}
