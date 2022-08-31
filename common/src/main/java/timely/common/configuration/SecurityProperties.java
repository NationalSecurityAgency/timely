package timely.common.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.validation.Valid;

import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.cloud.context.config.annotation.RefreshScope;

@RefreshScope
@ConfigurationProperties(prefix = "timely.security")
public class SecurityProperties {

    private boolean allowAnonymousHttpAccess = false;
    private boolean allowAnonymousWsAccess = false;
    private int sessionMaxAge = 86400;
    private int cacheExpirationMinutes = 60;
    private int cacheRefreshMinutes = 5;
    @NestedConfigurationProperty
    private SslJwtProperties jwtSsl = new SslJwtProperties();
    @Valid
    @NestedConfigurationProperty
    private SslServerProperties serverSsl = new SslServerProperties();

    private String authorizationUrl;
    private List<String> requiredRoles;
    private List<String> requiredAuths;

    private List<AuthorizedUser> authorizedUsers = new ArrayList<>();

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

    public SslServerProperties getServerSsl() {
        return serverSsl;
    }

    public SslJwtProperties getJwtSsl() {
        return jwtSsl;
    }

    public void setAuthorizedUsers(List<AuthorizedUser> authorizedUsers) {
        this.authorizedUsers = authorizedUsers;
    }

    public List<AuthorizedUser> getAuthorizedUsers() {
        return authorizedUsers;
    }

    public void setAuthorizationUrl(String authorizationUrl) {
        this.authorizationUrl = authorizationUrl;
    }

    public String getAuthorizationUrl() {
        return authorizationUrl;
    }

    public List<String> getRequiredAuths() {
        return requiredAuths;
    }

    public void setRequiredAuths(List<String> requiredAuths) {
        this.requiredAuths = requiredAuths;
    }

    public void setRequiredAuths(String requiredAuths) {
        if (StringUtils.isNotBlank(requiredAuths)) {
            this.requiredAuths = Arrays.stream(StringUtils.split(requiredAuths)).filter(Objects::nonNull).filter(s -> StringUtils.isNotBlank(s))
                            .map(String::trim).collect(Collectors.toList());
        }
    }

    public List<String> getRequiredRoles() {
        return requiredRoles;
    }

    public void setRequiredRoles(List<String> requiredRoles) {
        this.requiredRoles = requiredRoles;
    }

    public void setRequiredRoles(String requiredRoles) {
        if (StringUtils.isNotBlank(requiredRoles)) {
            this.requiredRoles = Arrays.stream(StringUtils.split(requiredRoles)).filter(Objects::nonNull).filter(s -> StringUtils.isNotBlank(s))
                            .map(String::trim).collect(Collectors.toList());
        }
    }
}
