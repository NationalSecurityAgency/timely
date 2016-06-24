package timely.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.util.Collections;
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
import org.apache.commons.configuration.BaseConfiguration;

import timely.Configuration;
import timely.api.model.Meta;

public class GetMetricTableSplitPoints {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("CreateMetricTableSplitPoints <configFile>");
            System.exit(1);
        }

        final File f = new File(args[0]);
        if (!f.canRead()) {
            throw new RuntimeException("Configuration file does not exist or cannot be read");
        }

        Configuration conf = new Configuration(f);
        final BaseConfiguration apacheConf = new BaseConfiguration();
        apacheConf.setProperty("instance.name", conf.get(Configuration.INSTANCE_NAME));
        apacheConf.setProperty("instance.zookeeper.host", conf.get(Configuration.ZOOKEEPERS));
        final ClientConfiguration aconf = new ClientConfiguration(Collections.singletonList(apacheConf));
        final Instance instance = new ZooKeeperInstance(aconf);
        final byte[] passwd = conf.get(Configuration.PASSWORD).getBytes(UTF_8);
        Connector con = instance.getConnector(conf.get(Configuration.USERNAME), new PasswordToken(passwd));
        Scanner s = con.createScanner(conf.get(Configuration.META_TABLE), con.securityOperations()
                .getUserAuthorizations(con.whoami()));
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
