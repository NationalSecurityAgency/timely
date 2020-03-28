package timely.balancer.configuration;

import javax.validation.Valid;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;
import timely.configuration.Cache;

@Validated
@Component
@ConfigurationProperties(prefix = "timely-balancer")
public class BalancerConfiguration {

    @Valid
    @NestedConfigurationProperty
    private BalancerServer server = new BalancerServer();

    @Valid
    @NestedConfigurationProperty
    private BalancerHttp http = new BalancerHttp();

    @Valid
    @NestedConfigurationProperty
    private BalancerWebsocket websocket = new BalancerWebsocket();

    @Valid
    @NestedConfigurationProperty
    private ZooKeeper zooKeeper = new ZooKeeper();

    @Valid
    @NestedConfigurationProperty
    private BalancerSecurity security = new BalancerSecurity();

    @Valid
    @NestedConfigurationProperty
    private Cache cache = new Cache();

    private boolean loginRequired = false;

    private String assignmentFile;

    private Double controlBandPercentage = 0.05;

    private long checkServerHealthInterval = 10000;

    private int serverFailuresBeforeDown = 3;

    private int serverSuccessesBeforeUp = 3;

    private String metricAssignmentPersisterType = "HDFS";

    private long balanceDelay = 900000;

    private long balancePeriod = 900000;

    private long persistDelay = 10000;

    private long persistPeriod = 60000;

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setLoginRequired(boolean loginRequired) {
        this.loginRequired = loginRequired;
    }

    public boolean isLoginRequired() {
        return loginRequired;
    }

    public String getAssignmentFile() {
        return assignmentFile;
    }

    public void setAssignmentFile(String assignmentFile) {
        this.assignmentFile = assignmentFile;
    }

    public void setControlBandPercentage(Double controlBandPercentage) {
        this.controlBandPercentage = controlBandPercentage;
    }

    public Double getControlBandPercentage() {
        return controlBandPercentage;
    }

    public BalancerSecurity getSecurity() {
        return security;
    }

    public BalancerServer getServer() {
        return server;
    }

    public BalancerHttp getHttp() {
        return http;
    }

    public BalancerWebsocket getWebsocket() {
        return websocket;
    }

    public Cache getCache() {
        return cache;
    }

    public void setCheckServerHealthInterval(long checkServerHealthInterval) {
        this.checkServerHealthInterval = checkServerHealthInterval;
    }

    public long getCheckServerHealthInterval() {
        return checkServerHealthInterval;
    }

    public int getServerFailuresBeforeDown() {
        return serverFailuresBeforeDown;
    }

    public void setServerFailuresBeforeDown(int serverFailuresBeforeDown) {
        this.serverFailuresBeforeDown = serverFailuresBeforeDown;
    }

    public int getServerSuccessesBeforeUp() {
        return serverSuccessesBeforeUp;
    }

    public void setServerSuccessesBeforeUp(int serverSuccessesBeforeUp) {
        this.serverSuccessesBeforeUp = serverSuccessesBeforeUp;
    }

    public String getMetricAssignmentPersisterType() {
        return metricAssignmentPersisterType;
    }

    public void setMetricAssignmentPersisterType(String metricAssignmentPersisterType) {
        this.metricAssignmentPersisterType = metricAssignmentPersisterType;
    }

    public void setBalanceDelay(long balanceDelay) {
        this.balanceDelay = balanceDelay;
    }

    public long getBalanceDelay() {
        return balanceDelay;
    }

    public void setBalancePeriod(long balancePeriod) {
        this.balancePeriod = balancePeriod;
    }

    public long getBalancePeriod() {
        return balancePeriod;
    }

    public void setPersistDelay(long persistDelay) {
        this.persistDelay = persistDelay;
    }

    public long getPersistDelay() {
        return persistDelay;
    }

    public void setPersistPeriod(long persistPeriod) {
        this.persistPeriod = persistPeriod;
    }

    public long getPersistPeriod() {
        return persistPeriod;
    }
}
