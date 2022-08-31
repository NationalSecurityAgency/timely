package timely.common.configuration;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

/**
 * Provides an {@link SSLContext} (JDK) or an {@link SslContext} (Netty) for use in the application.
 */
@Configuration
@EnableConfigurationProperties({SslServerProperties.class})
public class SslServerConfig {

    private static final Logger log = LoggerFactory.getLogger(SslServerConfig.class);

    @Bean
    protected SelfSignedCertificate selfSignedCertificate() throws Exception {
        Date begin = new Date();
        Date end = new Date(begin.getTime() + TimeUnit.DAYS.toMillis(7));
        return new SelfSignedCertificate("localhost", begin, end);
    }

    @Bean
    @Qualifier("nettySslContext")
    protected SslContext nettySslContext(SslServerProperties sslServerProperties, SelfSignedCertificate ssc) throws Exception {
        Boolean generate = sslServerProperties.isUseGeneratedKeypair();
        SslContextBuilder sslContextBuilder;
        if (generate) {
            log.warn("Using generated self signed server certificate");
            sslContextBuilder = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
        } else {
            sslContextBuilder = SslHelper.getSslContextBuilder(sslServerProperties);
        }

        // Can't set to REQUIRE because the CORS pre-flight requests will fail.
        sslContextBuilder.clientAuth(ClientAuth.OPTIONAL);
        return sslContextBuilder.build();
    }
}
