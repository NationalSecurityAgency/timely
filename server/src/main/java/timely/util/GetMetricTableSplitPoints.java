package timely.util;

import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import timely.common.configuration.AccumuloProperties;
import timely.common.configuration.TimelyProperties;
import timely.common.configuration.ZookeeperProperties;
import timely.model.Meta;
import timely.server.configuration.SpringBootstrap;

public class GetMetricTableSplitPoints {

    public static void main(String[] args) throws Exception {

        try (ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(SpringBootstrap.class).bannerMode(Mode.OFF)
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
            try (AccumuloClient accumuloClient = org.apache.accumulo.core.client.Accumulo.newClient().from(properties).build();
                            Scanner s = accumuloClient.createScanner(timelyProperties.getMetaTable(),
                                            accumuloClient.securityOperations().getUserAuthorizations(accumuloClient.whoami()))) {

                s.setRange(new Range(Meta.METRIC_PREFIX, true, Meta.TAG_PREFIX, false));
                for (Entry<Key,Value> e : s) {
                    System.out.println(e.getKey().getRow().toString().substring(Meta.METRIC_PREFIX.length()));
                }
            }
        }
    }
}
