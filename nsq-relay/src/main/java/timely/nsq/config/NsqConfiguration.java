package timely.nsq.config;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sproutsocial.nsq.Client;

@Configuration
@EnableConfigurationProperties(NsqProperties.class)
public class NsqConfiguration {

    @Bean
    public Client nsqClient(NsqProperties nsqProperties) {
        Client client = new Client();
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("message-handler-%d").build();
        client.setExecutor(Executors.newFixedThreadPool(nsqProperties.getMessageHandlerThreads(), threadFactory));
        return client;
    }
}
