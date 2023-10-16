package timely.store.compaction.util;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.ClientProperty;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import timely.configuration.Accumulo;
import timely.configuration.Configuration;
import timely.configuration.SpringBootstrap;

public class TabletMetadataConsole {

    public static void main(String[] args) throws Exception {
        try (ConfigurableApplicationContext ctx = new SpringApplicationBuilder(SpringBootstrap.class)
                .bannerMode(Banner.Mode.OFF).web(WebApplicationType.NONE).run(args)) {
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
                    .build()) {
                TabletMetadataQuery query = new TabletMetadataQuery(accumuloClient, conf.getMetricsTable());
                TabletMetadataView view = query.run();

                System.out.println(view.toText(TimeUnit.DAYS));
            }
        }
    }
}
