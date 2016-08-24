package timely.test;

import timely.Configuration;

public class TestConfiguration {

    public static final String TIMELY_HTTP_ADDRESS_DEFAULT = "localhost";

    public static Configuration createMinimalConfigurationForTest() {
        Configuration cfg = new Configuration();
        cfg.setIp("127.0.0.1");
        cfg.getPort().setPut(54321);
        cfg.getPort().setQuery(54322);
        cfg.getPort().setWebsocket(54323);
        cfg.setZookeepers("localhost:2181");
        cfg.setInstanceName("test");
        cfg.setUsername("root");
        cfg.setPassword("secret");
        cfg.getHttp().setHost("localhost");
        cfg.getSsl().setUseGeneratedKeypair(true);
        cfg.getWrite().setLatency("2s");
        cfg.getWebSocket().setTimeout(20);
        return cfg;
    }

}
