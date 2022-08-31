package timely.server.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import timely.common.configuration.MiniAccumuloProperties;
import timely.test.TestCaptureRequestHandler;

@Configuration
@ConditionalOnProperty(name = "timely.test", havingValue = "true")
@EnableConfigurationProperties({MiniAccumuloProperties.class})
public class TestTimelyConfiguration {

    @Bean
    @Qualifier("http")
    public TestCaptureRequestHandler httpCaptureRequestHandler() {
        return new TestCaptureRequestHandler(false);
    }

    @Bean
    @Qualifier("tcp")
    public TestCaptureRequestHandler tcpCaptureRequestHandler() {
        return new TestCaptureRequestHandler(false);
    }

    @Bean
    @Qualifier("udp")
    public TestCaptureRequestHandler udpCaptureRequestHandler() {
        return new TestCaptureRequestHandler(false);
    }
}
