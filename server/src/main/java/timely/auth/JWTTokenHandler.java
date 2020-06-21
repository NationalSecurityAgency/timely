package timely.auth;

import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.jsonwebtoken.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ResourceUtils;
import timely.configuration.Security;

/**
 * Converts between a String encoded JSON Web Token and a collection of
 * {@link TimelyUser}s.
 */
public class JWTTokenHandler {

    private static final Logger logger = LoggerFactory.getLogger(JWTTokenHandler.class);
    private static Key signatureCheckKey;
    private static ObjectMapper objectMapper;

    public static void init(Security security) {
        if (StringUtils.isNotBlank(security.getJwtCheckKeyStore())) {
            try {
                KeyStore keyStore = KeyStore
                        .getInstance(security.getJwtCheckKeyType() == null ? "JKS" : security.getJwtCheckKeyType());
                char[] keyPassword = security.getJwtCheckKeyPassword() == null ? null
                        : security.getJwtCheckKeyPassword().toCharArray();
                keyStore.load(ResourceUtils.getURL(security.getJwtCheckKeyStore()).openStream(), keyPassword);
                String alias = keyStore.aliases().nextElement();
                Certificate cert = keyStore.getCertificate(alias);
                JWTTokenHandler.signatureCheckKey = cert.getPublicKey();
                JWTTokenHandler.objectMapper = new ObjectMapper();
                JWTTokenHandler.objectMapper.registerModule(new GuavaModule());
            } catch (Exception e) {
                throw new IllegalStateException("Invalid SSL configuration.", e);
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
        return principalsClaim.stream().map(obj -> objectMapper.convertValue(obj, TimelyUser.class))
                .collect(Collectors.toList());
    }
}
