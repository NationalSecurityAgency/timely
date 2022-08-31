package timely.server.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "timely.loadtest")
public class LoadTestProperties {
    private String host = "localhost";
    private int tcpPort = 4242;
    private int httpPort = 4243;
    private String testDataFile = "/tmp/testdata";
    private int numWriteThreads = 8;
    private int numQueryThreads = 8;
    private long backlogMinutes = 60;
    private long samplePeriodMs = 1000;
    private long queryPeriodMinutes = 60;
    private long testDurationMins = 5;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getTcpPort() {
        return tcpPort;
    }

    public void setTcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public String getTestDataFile() {
        return testDataFile;
    }

    public void setTestDataFile(String testDataFile) {
        this.testDataFile = testDataFile;
    }

    public int getNumWriteThreads() {
        return numWriteThreads;
    }

    public void setNumWriteThreads(int numWriteThreads) {
        this.numWriteThreads = numWriteThreads;
    }

    public int getNumQueryThreads() {
        return numQueryThreads;
    }

    public void setNumQueryThreads(int numQueryThreads) {
        this.numQueryThreads = numQueryThreads;
    }

    public long getBacklogMinutes() {
        return backlogMinutes;
    }

    public void setBacklogMinutes(long backlogMinutes) {
        this.backlogMinutes = backlogMinutes;
    }

    public long getSamplePeriodMs() {
        return samplePeriodMs;
    }

    public void setSamplePeriodMs(long samplePeriodMs) {
        this.samplePeriodMs = samplePeriodMs;
    }

    public long getQueryPeriodMinutes() {
        return queryPeriodMinutes;
    }

    public void setQueryPeriodMinutes(long queryPeriodMinutes) {
        this.queryPeriodMinutes = queryPeriodMinutes;
    }

    public long getTestDurationMins() {
        return testDurationMins;
    }

    public void setTestDurationMins(long testDurationMins) {
        this.testDurationMins = testDurationMins;
    }
}
