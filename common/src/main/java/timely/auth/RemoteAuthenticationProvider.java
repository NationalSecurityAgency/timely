package timely.auth;

import static timely.common.component.AuthenticationService.PRINCIPALS_CLAIM;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.web.reactive.function.client.WebClient;

import timely.auth.util.ProxiedEntityUtils;
import timely.common.configuration.SecurityProperties;

/**
 * An {@link AuthenticationUserDetailsService} that retrieves user information from a remote authorization service for a set of proxied entity names, and
 * combines the results into a {@link TimelyPrincipal}.
 * <p>
 * This service assumes that the caller principal and proxied entities all need to be combined into a proxy chain that is authenticated. The purpose of this
 * service is for a microservice that has not received a JWT header to call out to a remote authorization service to retrieve authentication information. In
 * production, it is likely better for the JWT to be retrieved by a load balancer or other API gateway and inject the JWT header before calling a service.
 * Therefore, this service is mostly useful for debugging and if there are problems with the suggested approach.
 */
public class RemoteAuthenticationProvider implements AuthenticationProvider {

    private static final Logger log = LoggerFactory.getLogger(RemoteAuthenticationProvider.class);
    private final WebClient webClient;

    public static final String ENTITIES_HEADER = "X-ProxiedEntitiesChain";
    public static final String ISSUERS_HEADER = "X-ProxiedIssuersChain";

    public RemoteAuthenticationProvider(WebClient.Builder builder, SecurityProperties securityProperties) {
        log.debug("Creating RemoteAuthenticationProvider");
        this.webClient = builder.baseUrl(securityProperties.getAuthorizationUrl()).build();
    }

    public TimelyPrincipal fetchTimelyPrincipal(TimelyAuthenticationToken token) throws UsernameNotFoundException {
        log.debug("Authenticating {}", token.getTimelyPrincipal().getUsername());
        TimelyPrincipal principal = token.getTimelyPrincipal();
        try {
            // @formatter:off
            String jwt = webClient.get()
                .header(ENTITIES_HEADER, buildDNChain(principal, SubjectIssuerDNPair::subjectDN))
                .header(ISSUERS_HEADER, buildDNChain(principal, SubjectIssuerDNPair::issuerDN))
                .retrieve()
                .bodyToMono(String.class)
                .block();
            // @formatter:on
            log.debug("Authenticated {} with {} result", principal.getUsername(), (jwt == null) ? "null" : "non-null");
            if (jwt != null) {
                Collection<TimelyUser> principals = JWTTokenHandler.createUsersFromToken(jwt, PRINCIPALS_CLAIM);
                long createTime = principals.stream().map(TimelyUser::getCreationTime).min(Long::compareTo).orElse(System.currentTimeMillis());
                return new TimelyPrincipal(principals, createTime);
            } else {
                throw new UsernameNotFoundException("No entities found for " + principal.getUsername());
            }
        } catch (Exception e) {
            log.error("Failed performing lookup of {}: {}", principal.getUsername(), e);
            throw new UsernameNotFoundException(e.getMessage(), e);
        }
    }

    private String buildDNChain(TimelyPrincipal principal, Function<SubjectIssuerDNPair,String> dnFunc) {
        // @formatter:off
        return "<" +
            principal.getProxiedUsers().stream()
                .map(TimelyUser::getDn)
                .map(dnFunc)
                .map(ProxiedEntityUtils::buildProxiedDN)
                .collect(Collectors.joining("><"))
            + ">";
        // @formatter:on
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        if (!(authentication instanceof TimelyAuthenticationToken)) {
            throw new IllegalArgumentException(this.getClass().getCanonicalName() + " does not support " + authentication.getClass().getCanonicalName());
        }
        TimelyAuthenticationToken token = (TimelyAuthenticationToken) authentication;
        TimelyPrincipal timelyPrincipal = fetchTimelyPrincipal(token);
        return new PreAuthenticatedAuthenticationToken(timelyPrincipal, null);
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authentication.equals(TimelyAuthenticationToken.class);
    }
}
