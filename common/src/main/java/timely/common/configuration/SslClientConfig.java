package timely.common.configuration;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.netty.handler.ssl.SslContext;

/**
 * Provides an {@link SSLContext} (JDK) or an {@link SslContext} (Netty) for use in the application.
 */
@Configuration
@EnableConfigurationProperties({SslClientProperties.class})
@ConditionalOnExpression("!T(org.springframework.util.StringUtils).isEmpty('${timely.security.client-ssl.key-store-file:}')")
public class SslClientConfig {

    private static final Logger log = LoggerFactory.getLogger(SslClientConfig.class);

    @Bean
    @Qualifier("outboundJDKSslContext")
    public SSLContext outboundJDKSslContext(SslClientProperties sslClientProperties) throws Exception {
        log.debug("Creating outboundJDKSslContext using keyStoreFile:{} trustStoreFile:{}", sslClientProperties.getKeyStoreFile(),
                        sslClientProperties.getTrustStoreFile());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(SslHelper.getKeyManagerFactory(sslClientProperties).getKeyManagers(),
                        SslHelper.getTrustManagerFactory(sslClientProperties).getTrustManagers(), null);
        return sslContext;
    }

    @Bean
    @Qualifier("outboundNettySslContext")
    public SslContext outboundNettySslContext(SslClientProperties sslClientProperties) throws Exception {
        log.debug("Creating outboundNettySslContext using keyStoreFile:{} trustStoreFile:{}", sslClientProperties.getKeyStoreFile(),
                        sslClientProperties.getTrustStoreFile());
        return SslHelper.getSslContext(sslClientProperties);
    }
}
