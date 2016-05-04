package timely.auth;

import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

public class AuthenticationServiceTest {

    @Test(expected = BadCredentialsException.class)
    public void testBasicAuthenticationFailure() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("test", "test2");
        AuthenticationService.getAuthenticationManager().authenticate(token);
    }

    @Test
    public void testBasicAuthenticationLogin() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("test", "test1");
        Authentication auth = AuthenticationService.getAuthenticationManager().authenticate(token);
        Collection<? extends GrantedAuthority> authorizations = auth.getAuthorities();
        authorizations.forEach(a -> {
            Assert.assertTrue(a.getAuthority().equals("A") || a.getAuthority().equals("B")
                    || a.getAuthority().equals("C"));
        });
    }

    @Test
    public void testX509AuthenticationLogin() {
        PreAuthenticatedAuthenticationToken token = new PreAuthenticatedAuthenticationToken("example.com",
                "doesn't matter what I put here");
        Authentication auth = AuthenticationService.getAuthenticationManager().authenticate(token);
        Collection<? extends GrantedAuthority> authorizations = auth.getAuthorities();
        authorizations.forEach(a -> {
            Assert.assertTrue(a.getAuthority().equals("D") || a.getAuthority().equals("E")
                    || a.getAuthority().equals("F"));
        });
    }

    @Test(expected = UsernameNotFoundException.class)
    public void testX509AuthenticationLoginFailed() {
        PreAuthenticatedAuthenticationToken token = new PreAuthenticatedAuthenticationToken("bad.example.com",
                "doesn't matter what I put here");
        Authentication auth = AuthenticationService.getAuthenticationManager().authenticate(token);
        Collection<? extends GrantedAuthority> authorizations = auth.getAuthorities();
        authorizations.forEach(a -> {
            Assert.assertTrue(a.getAuthority().equals("D") || a.getAuthority().equals("E")
                    || a.getAuthority().equals("F"));
        });
    }

}
