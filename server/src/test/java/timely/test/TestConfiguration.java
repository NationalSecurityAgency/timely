package timely.test;

import java.util.HashMap;

import timely.configuration.Configuration;

public class TestConfiguration {

    public static final String TIMELY_HTTP_ADDRESS_DEFAULT = "localhost";

    public static final int WAIT_SECONDS = 2;

    public static Configuration createMinimalConfigurationForTest() {
        // @formatter:off
        Configuration cfg = new Configuration();
        cfg.getServer().setIp("127.0.0.1");
        cfg.getServer().setTcpPort(54321);
        cfg.getServer().setUdpPort(54325);
        cfg.getServer().setShutdownQuietPeriod(0);
        cfg.getHttp().setIp("127.0.0.1");
        cfg.getHttp().setPort(54322);
        cfg.getHttp().setHost("localhost");
        cfg.getWebsocket().setIp("127.0.0.1");
        cfg.getWebsocket().setPort(54323);
        cfg.getWebsocket().setTimeout(20);
        cfg.getAccumulo().setZookeepers("localhost:2181");
        cfg.getAccumulo().setInstanceName("test");
        cfg.getAccumulo().setUsername("root");
        cfg.getAccumulo().setPassword("secret");
        cfg.getAccumulo().getWrite().setLatency("100ms");
        cfg.getSecurity().getServerSsl().setUseGeneratedKeypair(true);
        HashMap<String,Integer> ageoff = new HashMap<>();
        ageoff.put("default", 10);
        cfg.setMetricAgeOffDays(ageoff);
        // @formatter:on

        return cfg;
    }

}
