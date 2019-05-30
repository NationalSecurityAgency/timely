package timely.configuration;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

public class Websocket {

    @NotBlank
    private String ip;
    @NotNull
    private Integer port;
    public int timeout = 60;
    public int subscriptionLag = 120;
    public int scannerBatchSize = 5000;
    public int flushIntervalSeconds = 30;
    public int scannerReadAhead = 1;
    public int subscriptionBatchSize = 1000;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getSubscriptionLag() {
        return subscriptionLag;
    }

    public void setSubscriptionLag(int subscriptionLag) {
        this.subscriptionLag = subscriptionLag;
    }

    public int getScannerBatchSize() {
        return this.scannerBatchSize;
    }

    public void setScannerBatchSize(int batchSize) {
        this.scannerBatchSize = batchSize;
    }

    public int getFlushIntervalSeconds() {
        return this.flushIntervalSeconds;
    }

    public void setFlushIntervalSeconds(int flushInterval) {
        this.flushIntervalSeconds = flushInterval;
    }

    public int getScannerReadAhead() {
        return this.scannerReadAhead;
    }

    public void setScannerReadAhead(int readAhead) {
        this.scannerReadAhead = readAhead;
    }

    public int getSubscriptionBatchSize() {
        return this.subscriptionBatchSize;
    }

    public void setSubscriptionBatchSize(int batchSize) {
        this.subscriptionBatchSize = batchSize;
    }
}
