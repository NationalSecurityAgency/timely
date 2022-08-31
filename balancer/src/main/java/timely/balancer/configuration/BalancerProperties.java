package timely.balancer.configuration;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@Validated
@Component
@ConfigurationProperties(prefix = "timely.balancer")
public class BalancerProperties {

    private boolean loginRequired = false;

    private String assignmentFile;

    private String defaultFs;

    private List<String> fsConfigResources;

    private Double controlBandPercentage = 0.05;

    private long checkServerHealthInterval = 10000;

    private int serverFailuresBeforeDown = 3;

    private int serverSuccessesBeforeUp = 3;

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

    public String getDefaultFs() {
        return defaultFs;
    }

    public void setDefaultFs(String defaultFs) {
        this.defaultFs = defaultFs;
    }

    public List<String> getFsConfigResources() {
        return fsConfigResources;
    }

    public void setFsConfigResources(List<String> fsConfigResources) {
        this.fsConfigResources = fsConfigResources;
    }

    public void setControlBandPercentage(Double controlBandPercentage) {
        this.controlBandPercentage = controlBandPercentage;
    }

    public Double getControlBandPercentage() {
        return controlBandPercentage;
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
}
