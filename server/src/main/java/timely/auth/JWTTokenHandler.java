package timely.auth;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.jsonwebtoken.*;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ResourceUtils;
import timely.configuration.Accumulo;
import timely.configuration.Security;

/**
 * Converts between a String encoded JSON Web Token and a collection of
 * {@link TimelyUser}s.
 */
public class JWTTokenHandler {

    private static final Logger logger = LoggerFactory.getLogger(JWTTokenHandler.class);
    private static Key signatureCheckKey;
    private static ObjectMapper objectMapper;
    private static Collection<String> accumuloAuths = new TreeSet<>();

    public static void init(Security security, Accumulo accumuloConfiguration) {
        if (StringUtils.isNotBlank(security.getJwtCheckKeyStore())) {
            try {
                String type = security.getJwtCheckKeyType();
                if (type != null && type.equals("X.509")) {
                    CertificateFactory factory = CertificateFactory.getInstance("X.509");
                    InputStream is = ResourceUtils.getURL(security.getJwtCheckKeyStore()).openStream();
                    X509Certificate certificate = (X509Certificate) factory.generateCertificate(is);
                    JWTTokenHandler.signatureCheckKey = certificate.getPublicKey();
                } else {
                    KeyStore keyStore = KeyStore.getInstance(type == null ? "JKS" : type);
                    char[] keyPassword = security.getJwtCheckKeyPassword() == null ? null
                            : security.getJwtCheckKeyPassword().toCharArray();
                    keyStore.load(ResourceUtils.getURL(security.getJwtCheckKeyStore()).openStream(), keyPassword);
                    String alias = keyStore.aliases().nextElement();
                    Certificate cert = keyStore.getCertificate(alias);
                    JWTTokenHandler.signatureCheckKey = cert.getPublicKey();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Invalid SSL configuration.", e);
            }
        }
        JWTTokenHandler.objectMapper = new ObjectMapper();
        JWTTokenHandler.objectMapper.registerModule(new GuavaModule());
        if (accumuloConfiguration != null) {
            try {
                final Properties properties = new Properties();
                properties.put(ClientProperty.INSTANCE_NAME.getKey(), accumuloConfiguration.getInstanceName());
                properties.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), accumuloConfiguration.getZookeepers());
                properties.put(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(),
                        accumuloConfiguration.getZookeeperTimeout());
                properties.put(ClientProperty.AUTH_PRINCIPAL.getKey(), accumuloConfiguration.getUsername());
                properties.put(ClientProperty.AUTH_TOKEN.getKey(), accumuloConfiguration.getPassword());
                properties.put(ClientProperty.AUTH_TYPE.getKey(), "password");
                try (AccumuloClient accumuloClient = org.apache.accumulo.core.client.Accumulo.newClient()
                        .from(properties).build()) {
                    Authorizations currentAccumuloAuths = accumuloClient.securityOperations()
                            .getUserAuthorizations(accumuloClient.whoami());
                    currentAccumuloAuths.iterator()
                            .forEachRemaining(a -> accumuloAuths.add(new String(a, Charset.forName("UTF-8"))));
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public static Collection<TimelyUser> createUsersFromToken(String token, String claimName) {
        logger.trace("Attempting to parse JWT {}", token);
        Jws<Claims> claimsJws = Jwts.parser().setSigningKey(signatureCheckKey).parseClaimsJws(token);
        Claims claims = claimsJws.getBody();
        logger.trace("Resulting claims: {}", claims);
        List<?> principalsClaim = claims.get(claimName, List.class);
        if (principalsClaim == null || principalsClaim.isEmpty()) {
            throw new IllegalArgumentException(
                    "JWT for " + claims.getSubject() + " does not contain any proxied principals.");
        }
        // convert to TimelyUser and downgrade auths to contain only what the
        // accumuloUser has
        return principalsClaim.stream().map(obj -> objectMapper.convertValue(obj, TimelyUser.class)).map(u -> {
            Collection<String> intersectedAuths = new ArrayList<>(u.getAuths());
            intersectedAuths.removeIf(a -> !accumuloAuths.contains(a));
            return new TimelyUser(u.getDn(), u.getUserType(), u.getEmail(), intersectedAuths, u.getRoles(),
                    u.getRoleToAuthMapping(), u.getCreationTime(), u.getExpirationTime());

        }).collect(Collectors.toList());
    }
}
