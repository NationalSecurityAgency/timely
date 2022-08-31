package timely.common.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.web.reactive.function.client.WebClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;

import io.netty.handler.ssl.SslContext;
import reactor.netty.http.client.HttpClient;

/**
 * Customizes the Spring-provided {@link org.springframework.web.reactive.function.client.WebClient.Builder} with an {@link SslContext} that will provide a
 * client certificate to the remote server if one is requested.
 */
@Configuration
@Order(100) // execute this after standard customizers to ensure that we overwrite the client connector
@ConditionalOnBean(name = "outboundNettySslContext")
public class ClientCertWebClientCustomizer {

    @Bean
    public WebClientCustomizer webClientCustomizer(@Qualifier("outboundNettySslContext") SslContext sslContext) {
        return webClientBuilder -> {
            HttpClient httpClient = HttpClient.create().secure(sslContextSpec -> sslContextSpec.sslContext(sslContext));
            ReactorClientHttpConnector clientHttpConnector = new ReactorClientHttpConnector(httpClient);
            webClientBuilder.clientConnector(clientHttpConnector);
        };
    }
}
