package timely.nsq;

import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.PreDestroy;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import com.sproutsocial.nsq.Client;
import com.sproutsocial.nsq.Config;
import com.sproutsocial.nsq.MessageDataHandler;
import com.sproutsocial.nsq.Subscriber;
import com.sproutsocial.nsq.SubscriptionId;

import timely.nsq.config.NsqProperties;
import timely.nsq.config.TimelyClientProperties;

@EnableScheduling
@Component
@EnableConfigurationProperties(TimelyClientProperties.class)
public class TimelyNsqSubscriber extends Subscriber {

    private final LinkedBlockingQueue<String> metricQueue;
    private final TimelyNsqRelayHandler relayHandler;
    private final SubscriptionId subscriptionId;

    public TimelyNsqSubscriber(Client nsqClient, NsqProperties nsqProperties, TimelyClientProperties timelyClientProperties) {
        super(nsqClient, nsqProperties.getLookupIntervalSecs(), nsqProperties.getMaxLookupFailuresBeforeError(),
                        nsqProperties.getLookupdHttpAddress().toArray(new String[0]));
        setDefaultMaxInFlight(nsqProperties.getMaxInFlight());
        setMaxAttempts(nsqProperties.getMaxDeliveryAttempts());
        Config config = new Config();
        config.setUserAgent(TimelyNsqSubscriber.class.getSimpleName());
        setConfig(config);
        this.metricQueue = new LinkedBlockingQueue<>(nsqProperties.getMessageHandlerQueueSize());
        MessageDataHandler messageDataHandler = new TimelyNsqMessageDataHandler(this.metricQueue);
        this.relayHandler = new TimelyNsqRelayHandler(timelyClientProperties, this.metricQueue);
        this.subscriptionId = subscribe(nsqProperties.getTopic(), nsqProperties.getChannel(), messageDataHandler);
    }

    @PreDestroy
    public void shutdown() {
        unsubscribe(this.subscriptionId);
        this.relayHandler.shutdown();
    }
}
