package timely.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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

            final Map<String, String> properties = new HashMap<>();
            Accumulo accumuloConf = conf.getAccumulo();
            properties.put("instance.name", accumuloConf.getInstanceName());
            properties.put("instance.zookeeper.host", accumuloConf.getZookeepers());
            final ClientConfiguration aconf = ClientConfiguration.fromMap(properties);
            final Instance instance = new ZooKeeperInstance(aconf);
            Connector con = instance.getConnector(accumuloConf.getUsername(),
                    new PasswordToken(accumuloConf.getPassword()));
            Scanner s = con.createScanner(conf.getMetaTable(),
                    con.securityOperations().getUserAuthorizations(con.whoami()));
            try {
                s.setRange(new Range(Meta.METRIC_PREFIX, true, Meta.TAG_PREFIX, false));
                for (Entry<Key, Value> e : s) {
                    System.out.println(e.getKey().getRow().toString().substring(Meta.METRIC_PREFIX.length()));
                }
            } finally {
                s.close();
            }
        }
    }
}
