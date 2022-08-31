package timely.store.compaction.util;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.ClientProperty;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import timely.common.configuration.AccumuloProperties;
import timely.common.configuration.TimelyProperties;
import timely.common.configuration.ZookeeperProperties;
import timely.server.configuration.SpringBootstrap;

public class TabletMetadataConsole {

    public static void main(String[] args) throws Exception {
        try (ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(SpringBootstrap.class).bannerMode(Banner.Mode.OFF)
                        .web(WebApplicationType.NONE).run(args)) {
            TimelyProperties timelyProperties = applicationContext.getBean(TimelyProperties.class);
            AccumuloProperties accumuloProperties = applicationContext.getBean(AccumuloProperties.class);
            ZookeeperProperties zookeeperProperties = applicationContext.getBean(ZookeeperProperties.class);

            final Properties properties = new Properties();
            properties.put(ClientProperty.INSTANCE_NAME.getKey(), accumuloProperties.getInstanceName());
            properties.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), zookeeperProperties.getServers());
            properties.put(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(), zookeeperProperties.getTimeout());
            properties.put(ClientProperty.AUTH_PRINCIPAL.getKey(), accumuloProperties.getUsername());
            properties.put(ClientProperty.AUTH_TOKEN.getKey(), accumuloProperties.getPassword());
            properties.put(ClientProperty.AUTH_TYPE.getKey(), "password");
            try (AccumuloClient accumuloClient = org.apache.accumulo.core.client.Accumulo.newClient().from(properties).build()) {
                TabletMetadataQuery query = new TabletMetadataQuery(accumuloClient, timelyProperties.getMetricsTable());
                TabletMetadataView view = query.run();

                System.out.println(view.toText(TimeUnit.DAYS));
            }
        }
    }
}
