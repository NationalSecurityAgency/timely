package timely.test;

import timely.Configuration;

public class TestConfiguration {

    public static final String TIMELY_HTTP_ADDRESS_DEFAULT = "localhost";

    public static Configuration createMinimalConfigurationForTest() {
        Configuration cfg = new Configuration().setIp("127.0.0.1").getPort().setPut(54321).getPort().setQuery(54322)
                .getPort().setWebsocket(54323).setZookeepers("localhost:2181").setInstanceName("test")
                .setUsername("root").setPassword("secret").getHttp().setHost("localhost").getSsl()
                .setUseGeneratedKeypair(true).getWrite().setLatency("2s").getWebSocket().setTimeout(20);
        return cfg;
    }

}
