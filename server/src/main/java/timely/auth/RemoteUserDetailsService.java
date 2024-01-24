package timely.auth;

import static timely.auth.AuthenticationService.PRINCIPALS_CLAIM;

import java.util.ArrayList;
import java.util.Collection;

import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import timely.auth.util.SslHelper;
import timely.configuration.AuthorizationProperties;
import timely.configuration.ClientSsl;
import timely.netty.http.auth.TimelyAuthenticationToken;
import timely.netty.http.auth.TimelyUserDetails;

public class RemoteUserDetailsService implements AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> {

    static private final Logger log = LoggerFactory.getLogger(RemoteUserDetailsService.class);
    private WebClient webClient;
    private ClientSsl clientSsl;
    private AuthorizationProperties authorizationProperties;

    public RemoteUserDetailsService() {

    }

    public RemoteUserDetailsService(ClientSsl clientSsl, AuthorizationProperties authorizationProperties)
            throws Exception {
        this.clientSsl = clientSsl;
        this.authorizationProperties = authorizationProperties;
        initialize();
    }

    public void initialize() throws Exception {
        WebClient.Builder webClientBuilder = WebClient.builder();
        SslContext sslContext = SslHelper.getSslContext(clientSsl);
        HttpClient httpClient = HttpClient.create().secure(sslContextSpec -> sslContextSpec.sslContext(sslContext));
        ClientHttpConnector httpConnector = new ReactorClientHttpConnector(httpClient);
        this.webClient = webClientBuilder.clientConnector(httpConnector)
                .baseUrl(authorizationProperties.getAuthorizationUrl()).build();
    }

    public void setClientSsl(ClientSsl clientSsl) {
        this.clientSsl = clientSsl;
    }

    public void setAuthorizationProperties(AuthorizationProperties authorizationProperties) {
        this.authorizationProperties = authorizationProperties;
    }

    @Override
    public UserDetails loadUserDetails(PreAuthenticatedAuthenticationToken token) throws UsernameNotFoundException {
        TimelyUserDetails userDetails = null;
        if (!(token instanceof TimelyAuthenticationToken)) {
            log.error("Unexpected type for token: {}", token.getClass().getName());
        }
        TimelyAuthenticationToken timelyToken = (TimelyAuthenticationToken) token;
        TimelyPrincipal principal = timelyToken.getTimelyPrincipal();
        String subjectDN = principal.getPrimaryUser().getDn().subjectDN();
        String issuerDN = principal.getPrimaryUser().getDn().issuerDN();
        try {
            log.trace("Authenticating {} via the authorization microservice", subjectDN);
            // @formatter:off
            String jwt = webClient.get()
                    .header(TimelyAuthenticationToken.PROXIED_ENTITIES_HEADER, subjectDN)
                    .header(TimelyAuthenticationToken.PROXIED_ISSUERS_HEADER, issuerDN)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
            // @formatter:on
            log.trace("Authenticated {} via the authorization microservice with {} result", subjectDN,
                    jwt == null ? "null" : "non-null");
            if (jwt != null) {
                Collection<TimelyUser> timelyUsers = JWTTokenHandler.createUsersFromToken(jwt, PRINCIPALS_CLAIM);
                if (timelyUsers.iterator().hasNext()) {
                    userDetails = getTimelyUserDetails(timelyUsers.iterator().next());
                }
            }
            if (userDetails == null) {
                throw new UsernameNotFoundException("No entities found for " + subjectDN);
            }
        } catch (Exception e) {
            log.error("Failed performing lookup of {}: {}", subjectDN, e);
            throw new UsernameNotFoundException(e.getMessage(), e);
        }
        return userDetails;
    }

    private TimelyUserDetails getTimelyUserDetails(TimelyUser timelyUser) {
        return new TimelyUserDetails() {

            private static final long serialVersionUID = 1L;

            @Override
            public Collection<SimpleGrantedAuthority> getAuthorities() {
                final Collection<SimpleGrantedAuthority> auths = new ArrayList<>();
                timelyUser.getAuths().forEach(a -> {
                    auths.add(new SimpleGrantedAuthority(a));
                });
                return auths;
            }

            @Override
            public Collection<String> getRoles() {
                final Collection<String> roles = new ArrayList<>();
                roles.addAll(timelyUser.getRoles());
                return roles;
            }

            @Override
            public String getPassword() {
                return "";
            }

            @Override
            public String getUsername() {
                return timelyUser.getName();
            }

            @Override
            public boolean isAccountNonExpired() {
                return true;
            }

            @Override
            public boolean isAccountNonLocked() {
                return true;
            }

            @Override
            public boolean isCredentialsNonExpired() {
                return true;
            }

            @Override
            public boolean isEnabled() {
                return true;
            }

        };
    }
}
