package timely.auth;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import timely.common.configuration.SecurityProperties;

/**
 * Converts between a String encoded JSON Web Token and a collection of {@link TimelyUser}s.
 */
public class JWTTokenHandler {

    private static final Logger log = LoggerFactory.getLogger(JWTTokenHandler.class);
    private static Key signatureCheckKey;
    private static ObjectMapper objectMapper;
    private static Collection<String> accumuloAuths = new TreeSet<>();

    public static void init(SecurityProperties securityProperties, AccumuloClient accumuloClient) {
        if (StringUtils.isNotBlank(securityProperties.getJwtSsl().getKeyStoreFile())) {
            try {
                String type = securityProperties.getJwtSsl().getKeyStoreType();
                if (type != null && type.equals("X.509")) {
                    CertificateFactory factory = CertificateFactory.getInstance("X.509");
                    InputStream is = ResourceUtils.getURL(securityProperties.getJwtSsl().getKeyStoreFile()).openStream();
                    X509Certificate certificate = (X509Certificate) factory.generateCertificate(is);
                    JWTTokenHandler.signatureCheckKey = certificate.getPublicKey();
                } else {
                    KeyStore keyStore = KeyStore.getInstance(type == null ? "JKS" : type);
                    char[] keyPassword = securityProperties.getJwtSsl().getKeyStorePassword() == null ? null
                                    : securityProperties.getJwtSsl().getKeyStorePassword().toCharArray();
                    keyStore.load(ResourceUtils.getURL(securityProperties.getJwtSsl().getKeyStoreFile()).openStream(), keyPassword);
                    String alias = keyStore.aliases().nextElement();
                    Certificate certificate = keyStore.getCertificate(alias);
                    JWTTokenHandler.signatureCheckKey = certificate.getPublicKey();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Invalid SSL configuration.", e);
            }
        }
        JWTTokenHandler.objectMapper = new ObjectMapper();
        JWTTokenHandler.objectMapper.registerModule(new GuavaModule());
        if (accumuloClient != null) {
            try {
                Authorizations currentAccumuloAuths = accumuloClient.securityOperations().getUserAuthorizations(accumuloClient.whoami());
                currentAccumuloAuths.iterator().forEachRemaining(a -> accumuloAuths.add(new String(a, Charset.forName("UTF-8"))));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static Collection<TimelyUser> createUsersFromToken(String token, String claimName) {
        log.trace("Attempting to parse JWT {}", token);
        JwtParser jwtParser = Jwts.parser().setSigningKey(signatureCheckKey).build();
        Jws<Claims> claimsJws = jwtParser.parseClaimsJws(token);
        Claims claims = claimsJws.getBody();
        log.trace("Resulting claims: {}", claims);
        List<?> principalsClaim = claims.get(claimName, List.class);
        if (principalsClaim == null || principalsClaim.isEmpty()) {
            throw new IllegalArgumentException("JWT for " + claims.getSubject() + " does not contain any proxied principals.");
        }
        // convert to TimelyUser and downgrade auths to contain only what the
        // accumuloUser has
        return principalsClaim.stream().map(obj -> objectMapper.convertValue(obj, TimelyUser.class)).map(u -> {
            Collection<String> intersectedAuths = new ArrayList<>(u.getAuths());
            intersectedAuths.removeIf(a -> !accumuloAuths.contains(a));
            return new TimelyUser(u.getDn(), u.getUserType(), u.getEmail(), intersectedAuths, u.getRoles(), u.getRoleToAuthMapping(), u.getCreationTime(),
                            u.getExpirationTime());

        }).collect(Collectors.toList());
    }
}
