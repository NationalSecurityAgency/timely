package timely.test.configuration;

import java.util.Arrays;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "timely.in-memory-accumulo")
@ConditionalOnProperty(name = "timely.in-memory-accumulo.enabled", havingValue = "true")
public class InMemoryAccumuloProperties {

    private boolean enabled = false;
    private String rootAuths;

    public boolean getEnabled() {
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
