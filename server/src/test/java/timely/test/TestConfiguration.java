package timely.test;

import timely.Configuration;

public class TestConfiguration {

    public static final String TIMELY_HTTP_ADDRESS_DEFAULT = "localhost";

    public static Configuration createMinimalConfigurationForTest() {
        Configuration cfg = new Configuration().getServer().setIp("127.0.0.1").getServer().setTcpPort(54321).getHttp()
                .setIp("127.0.0.1").getHttp().setPort(54322).getWebsocket().setIp("127.0.0.1").getWebsocket()
                .setPort(54323).getAccumulo().setZookeepers("localhost:2181").getAccumulo().setInstanceName("test")
                .getAccumulo().setUsername("root").getAccumulo().setPassword("secret").getHttp().setHost("localhost")
                .getSecurity().getSsl().setUseGeneratedKeypair(true).getAccumulo().getWrite().setLatency("2s")
                .getWebsocket().setTimeout(20);
        return cfg;
    }

}
