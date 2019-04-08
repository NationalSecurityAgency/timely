package timely.configuration;

import javax.validation.Valid;

import org.springframework.boot.context.properties.NestedConfigurationProperty;

public class Security {

    private boolean allowAnonymousHttpAccess = false;
    private boolean allowAnonymousWsAccess = false;
    private int sessionMaxAge = 86400;
    private int cacheExpirationMinutes = 60;
    private int cacheRefreshMinutes = 5;
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

    public void setCacheExpirationMinutes(int cacheExpirationMinutes) {
        this.cacheExpirationMinutes = cacheExpirationMinutes;
    }

    public int getCacheExpirationMinutes() {
        return cacheExpirationMinutes;
    }

    public void setCacheRefreshMinutes(int cacheRefreshMinutes) {
        this.cacheRefreshMinutes = cacheRefreshMinutes;
    }

    public int getCacheRefreshMinutes() {
        return cacheRefreshMinutes;
    }

    public ServerSsl getServerSsl() {
        return serverSsl;
    }
}
