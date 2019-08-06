package timely.store.compaction.util;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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
            HashMap<String, String> apacheConf = new HashMap<>();
            Accumulo accumuloConf = conf.getAccumulo();
            apacheConf.put("instance.name", accumuloConf.getInstanceName());
            apacheConf.put("instance.zookeeper.host", accumuloConf.getZookeepers());
            ClientConfiguration aconf = ClientConfiguration.fromMap(apacheConf);
            Instance instance = new ZooKeeperInstance(aconf);
            Connector con = instance.getConnector(accumuloConf.getUsername(),
                    new PasswordToken(accumuloConf.getPassword()));

            TabletMetadataQuery query = new TabletMetadataQuery(con, conf.getMetricsTable());
            TabletMetadataView view = query.run();

            System.out.println(view.toText(TimeUnit.DAYS));
        }
    }
}
