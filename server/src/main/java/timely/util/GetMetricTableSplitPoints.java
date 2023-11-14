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
import timely.api.model.Meta;
import timely.configuration.Accumulo;
import timely.configuration.Configuration;
import timely.configuration.SpringBootstrap;

public class GetMetricTableSplitPoints {

    public static void main(String[] args) throws Exception {

        try (ConfigurableApplicationContext ctx = new SpringApplicationBuilder(SpringBootstrap.class)
                .bannerMode(Mode.OFF).web(WebApplicationType.NONE).run(args)) {
            Configuration conf = ctx.getBean(Configuration.class);

            final Properties properties = new Properties();
            Accumulo accumuloConf = conf.getAccumulo();
            properties.put(ClientProperty.INSTANCE_NAME.getKey(), accumuloConf.getInstanceName());
            properties.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), accumuloConf.getZookeepers());
            properties.put(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(), accumuloConf.getZookeeperTimeout());
            properties.put(ClientProperty.AUTH_PRINCIPAL.getKey(), accumuloConf.getUsername());
            properties.put(ClientProperty.AUTH_TOKEN.getKey(), accumuloConf.getPassword());
            properties.put(ClientProperty.AUTH_TYPE.getKey(), "password");
            try (AccumuloClient accumuloClient = org.apache.accumulo.core.client.Accumulo.newClient().from(properties)
                    .build();
                    Scanner s = accumuloClient.createScanner(conf.getMetaTable(),
                            accumuloClient.securityOperations().getUserAuthorizations(accumuloClient.whoami()))) {

                s.setRange(new Range(Meta.METRIC_PREFIX, true, Meta.TAG_PREFIX, false));
                for (Entry<Key, Value> e : s) {
                    System.out.println(e.getKey().getRow().toString().substring(Meta.METRIC_PREFIX.length()));
                }
            }
        }
    }
}
