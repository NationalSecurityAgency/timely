package timely.common.configuration;

import java.util.Arrays;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

@RefreshScope
@ConfigurationProperties(prefix = "timely.mini-accumulo")
public class MiniAccumuloProperties {

    private boolean enabled = false;
    private String rootAuths;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getRootAuths() {
        return rootAuths;
    }

    public Authorizations getRootAuthorizations() {
        if (rootAuths == null) {
            return new Authorizations();
        } else {
            return new Authorizations(Arrays.stream(StringUtils.split(rootAuths, ',')).map(String::trim).toArray(String[]::new));
        }
    }

    public void setRootAuths(String rootAuths) {
        this.rootAuths = rootAuths;
    }
}
