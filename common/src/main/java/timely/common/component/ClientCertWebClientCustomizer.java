package timely.common.component;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.web.reactive.function.client.WebClientCustomizer;
import org.springframework.core.annotation.Order;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import io.netty.handler.ssl.SslContext;
import reactor.netty.http.client.HttpClient;

/**
 * Customizes the Spring-provided {@link org.springframework.web.reactive.function.client.WebClient.Builder} in order to supply an {@link SslContext} that will
 * provide a client certificate to the remote server if one is requested.
 */
@Component
@Order(100) // execute this after standard customizers so we're sure to overwrite the client
            // connector
@ConditionalOnBean(name = "outboundNettySslContext")
public class ClientCertWebClientCustomizer implements WebClientCustomizer {

    private final SslContext sslContext;

    public ClientCertWebClientCustomizer(@Qualifier("outboundNettySslContext") SslContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    public void customize(WebClient.Builder webClientBuilder) {
        HttpClient httpClient = HttpClient.create().secure(sslContextSpec -> sslContextSpec.sslContext(sslContext));
        ReactorClientHttpConnector clientHttpConnector = new ReactorClientHttpConnector(httpClient);
        webClientBuilder.clientConnector(clientHttpConnector);
    }
}
