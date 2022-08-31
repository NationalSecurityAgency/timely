package timely.common.configuration;

import javax.net.ssl.SSLContext;

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
@ConditionalOnExpression("!T(org.springframework.util.StringUtils).isEmpty('${timely.security.clientSsl.keyStoreFile:}')")
public class SslClientConfig {

    @Bean
    @Qualifier("outboundJDKSslContext")
    public SSLContext outboundJDKSslContext(SslClientProperties sslClientProperties) throws Exception {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(SslHelper.getKeyManagerFactory(sslClientProperties).getKeyManagers(),
                        SslHelper.getTrustManagerFactory(sslClientProperties).getTrustManagers(), null);
        return sslContext;
    }

    @Bean
    @Qualifier("outboundNettySslContext")
    public SslContext outboundNettySslContext(SslClientProperties sslClientProperties) throws Exception {
        return SslHelper.getSslContext(sslClientProperties);
    }
}
