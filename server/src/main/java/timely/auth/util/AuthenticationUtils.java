package timely.auth.util;

import com.google.common.collect.HashMultimap;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyPrincipal;
import timely.auth.TimelyUser;
import timely.netty.http.auth.TimelyAuthenticationToken;

import java.security.cert.X509Certificate;
import java.util.Collection;

import java.util.stream.Collectors;

public class AuthenticationUtils {

    public static Collection<String> getAuthCollection(Authentication authentication) {
        Collection<? extends GrantedAuthority> authorities = authentication.getAuthorities();
        return authorities.stream().map(a -> ((GrantedAuthority) a).getAuthority()).collect(Collectors.toList());
    }

    public static TimelyAuthenticationToken getTimelyAuthenticationToken(Authentication authentication,
            X509Certificate clientCert) {
        TimelyAuthenticationToken timelyAuthenticationToken = null;
        if (authentication instanceof TimelyAuthenticationToken) {
            return (TimelyAuthenticationToken)authentication;
        } else {
            if (clientCert == null) {
                timelyAuthenticationToken = new TimelyAuthenticationToken(authentication.getName(), null,
                        HashMultimap.create());
                TimelyUser timelyUser = new TimelyUser(SubjectIssuerDNPair.of(authentication.getName()),
                        TimelyUser.UserType.USER, AuthenticationUtils.getAuthCollection(authentication), null, null,
                        System.currentTimeMillis());
                timelyAuthenticationToken.setTimelyPrincipal(new TimelyPrincipal(timelyUser));
            } else {
                timelyAuthenticationToken = new TimelyAuthenticationToken(authentication.getName(), clientCert,
                        HashMultimap.create());
                TimelyUser.UserType userType = DnUtils.isServerDN(clientCert.getSubjectDN().getName())
                        ? TimelyUser.UserType.SERVER
                        : TimelyUser.UserType.USER;
                TimelyUser timelyUser = new TimelyUser(SubjectIssuerDNPair.of(authentication.getName()),
                        userType, AuthenticationUtils.getAuthCollection(authentication), null, null,
                        System.currentTimeMillis());
                timelyAuthenticationToken.setTimelyPrincipal(new TimelyPrincipal(timelyUser));
            }
        }
        return timelyAuthenticationToken;
    }
}