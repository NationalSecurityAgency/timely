package timely.test;

import java.util.HashMap;

import timely.Configuration;

public class TestConfiguration {

    public static final String TIMELY_HTTP_ADDRESS_DEFAULT = "localhost";

    public static Configuration createMinimalConfigurationForTest() {
        // @formatter:off
        Configuration cfg = new Configuration()
                .getServer().setIp("127.0.0.1")
                .getServer().setTcpPort(54321)
                .getServer().setUdpPort(54325)
                .getHttp().setIp("127.0.0.1")
                .getHttp().setPort(54322)
                .getHttp().setHost("localhost")
                .getWebsocket().setIp("127.0.0.1")
                .getWebsocket().setPort(54323)
                .getWebsocket().setTimeout(20)
                .getAccumulo().setZookeepers("localhost:2181")
                .getAccumulo().setInstanceName("test")
                .getAccumulo().setUsername("root")
                .getAccumulo().setPassword("secret")
                .getAccumulo().getWrite().setLatency("2s")
                .getSecurity().getSsl().setUseGeneratedKeypair(true);
        HashMap<String,Integer> ageoff = new HashMap<>();
        ageoff.put("default", 10);
        cfg.setMetricAgeOffDays(ageoff);
        // @formatter:on

        return cfg;
    }

}
