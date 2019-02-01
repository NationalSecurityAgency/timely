package timely.configuration;

import javax.validation.Valid;

import org.springframework.boot.context.properties.NestedConfigurationProperty;

public class Security {

    private boolean allowAnonymousHttpAccess = false;
    private boolean allowAnonymousWsAccess = false;
    private int sessionMaxAge = 86400;
    @Valid
    @NestedConfigurationProperty
    private ServerSsl serverSsl = new ServerSsl();

    public boolean isAllowAnonymousHttpAccess() {
        return allowAnonymousHttpAccess;
    }

    public void setAllowAnonymousHttpAccess(boolean allowAnonymousHttpAccess) {
        this.allowAnonymousHttpAccess = allowAnonymousHttpAccess;
    }

    public boolean isAllowAnonymousWsAccess() {
        return allowAnonymousWsAccess;
    }

    public void setAllowAnonymousWsAccess(boolean allowAnonymousWsAccess) {
        this.allowAnonymousWsAccess = allowAnonymousWsAccess;
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
