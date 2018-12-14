package timely.configuration;

import javax.validation.Valid;

import org.springframework.boot.context.properties.NestedConfigurationProperty;

public class Security {

    private boolean allowAnonymousAccess = false;
    private int sessionMaxAge = 86400;
    @Valid
    @NestedConfigurationProperty
    private ServerSsl serverSsl = new ServerSsl();

    public boolean isAllowAnonymousAccess() {
        return allowAnonymousAccess;
    }

    public void setAllowAnonymousAccess(boolean allowAnonymousAccess) {
        this.allowAnonymousAccess = allowAnonymousAccess;
    }

    public int getSessionMaxAge() {
        return sessionMaxAge;
    }

    public void setSessionMaxAge(int sessionMaxAge) {
        this.sessionMaxAge = sessionMaxAge;
    }

    public ServerSsl getServerSsl() {
        return serverSsl;
    }
}
