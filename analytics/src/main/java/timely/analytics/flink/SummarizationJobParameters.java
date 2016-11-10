package timely.analytics.flink;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SummarizationJobParameters extends GlobalJobParameters implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String timelyHostname;
    private final int timelyTcpPort;
    private final int timelyHttpsPort;
    private final int timelyWssPort;
    private final boolean doLogin;
    private final String timelyUsername;
    private final String timelyPassword;
    private final String keyStoreFile;
    private final String keyStoreType;
    private final String keyStorePass;
    private final String trustStoreFile;
    private final String trustStoreType;
    private final String trustStorePass;
    private final boolean hostVerificationEnabled;
    private final int bufferSize;
    private final String[] metrics;
    private final long startTime;
    private final long endTime;
    private final String interval;
    private final String intervalUnits;

    public SummarizationJobParameters(ParameterTool params) {
        timelyHostname = params.getRequired("timelyHostname");
        timelyTcpPort = params.getInt("timelyTcpPort", 4241);
        timelyHttpsPort = params.getInt("timelyHttpsPort", 4242);
        timelyWssPort = params.getInt("timelyWssPort", 4243);
        doLogin = params.getBoolean("doLogin", false);
        timelyUsername = params.get("timelyUsername", null);
        timelyPassword = params.get("timelyPassword", null);
        keyStoreFile = params.getRequired("keyStoreFile");
        keyStoreType = params.get("keyStoreType", "JKS");
        keyStorePass = params.getRequired("keyStorePass");
        trustStoreFile = params.getRequired("trustStoreFile");
        trustStoreType = params.get("trustStoreType", "JKS");
        trustStorePass = params.getRequired("trustStorePass");
        hostVerificationEnabled = params.getBoolean("hostVerificationEnabled", true);
        bufferSize = params.getInt("bufferSize", 10485760);
        String metricNames = params.getRequired("metrics");
        if (null != metricNames) {
            metrics = metricNames.split(",");
        } else {
            metrics = null;
        }
        startTime = params.getLong("startTime", 0L);
        endTime = params.getLong("endTime", 0L);
        interval = params.getRequired("interval");
        intervalUnits = params.getRequired("intervalUnits");
    }

    public SummarizationJobParameters(Configuration conf) {
        System.out.println("Creating job parameters from configuration: " + conf);
        timelyHostname = conf.getString("timelyHostname", null);
        timelyTcpPort = conf.getInteger("timelyTcpPort", 4241);
        timelyHttpsPort = conf.getInteger("timelyHttpsPort", 4242);
        timelyWssPort = conf.getInteger("timelyWssPort", 4243);
        doLogin = conf.getBoolean("doLogin", false);
        timelyUsername = conf.getString("timelyUsername", null);
        timelyPassword = conf.getString("timelyPassword", null);
        keyStoreFile = conf.getString("keyStoreFile", null);
        keyStoreType = conf.getString("keyStoreType", "JKS");
        keyStorePass = conf.getString("keyStorePass", null);
        trustStoreFile = conf.getString("trustStoreFile", null);
        trustStoreType = conf.getString("trustStoreType", "JKS");
        trustStorePass = conf.getString("trustStorePass", null);
        hostVerificationEnabled = conf.getBoolean("hostVerificationEnabled", true);
        bufferSize = conf.getInteger("bufferSize", 10485760);
        String metricNames = conf.getString("metrics", null);
        if (null != metricNames) {
            metrics = metricNames.split(",");
        } else {
            metrics = null;
        }
        startTime = conf.getLong("startTime", 0L);
        endTime = conf.getLong("endTime", 0L);
        interval = conf.getString("interval", null);
        intervalUnits = conf.getString("intervalUnits", null);
    }

    public Time getSummarizationInterval() {
        return Time.of(Long.parseLong(interval), TimeUnit.valueOf(intervalUnits));
    }

    public String getTimelyHostname() {
        return timelyHostname;
    }

    public int getTimelyTcpPort() {
        return timelyTcpPort;
    }

    public int getTimelyHttpsPort() {
        return timelyHttpsPort;
    }

    public int getTimelyWssPort() {
        return timelyWssPort;
    }

    public boolean isDoLogin() {
        return doLogin;
    }

    public String getTimelyUsername() {
        return timelyUsername;
    }

    public String getTimelyPassword() {
        return timelyPassword;
    }

    public String getKeyStoreFile() {
        return keyStoreFile;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public String getKeyStorePass() {
        return keyStorePass;
    }

    public String getTrustStoreFile() {
        return trustStoreFile;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public String getTrustStorePass() {
        return trustStorePass;
    }

    public boolean isHostVerificationEnabled() {
        return hostVerificationEnabled;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public String[] getMetrics() {
        return metrics;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getInterval() {
        return interval;
    }

    public String getIntervalUnits() {
        return intervalUnits;
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> m = new HashMap<>();
        m.put("timelyHostname", this.timelyHostname);
        m.put("timelyHttpsPort", Integer.toString(this.timelyHttpsPort));
        m.put("timelyWssPort", Integer.toString(this.timelyWssPort));
        m.put("doLogin", Boolean.toString(this.doLogin));
        m.put("timelyUsername", this.timelyUsername);
        m.put("timelyPassword", this.timelyPassword);
        m.put("keyStoreFile", this.keyStoreFile);
        m.put("keyStoreType", this.keyStoreType);
        m.put("keyStorePass", this.keyStorePass);
        m.put("trustStoreFile", this.trustStoreFile);
        m.put("trustStoreType", this.trustStoreType);
        m.put("trustStorePass", this.trustStorePass);
        m.put("hostVerificationEnabled", Boolean.toString(this.hostVerificationEnabled));
        m.put("bufferSize", Integer.toString(this.bufferSize));
        m.put("metrics", String.join(",", this.metrics));
        m.put("startTime", Long.toString(this.startTime));
        m.put("endTime", Long.toString(this.endTime));
        m.put("interval", this.interval);
        m.put("intervalUnits", this.intervalUnits);
        return m;
    }

    @Override
    public String toString() {
        return toMap().toString();
    }
}
