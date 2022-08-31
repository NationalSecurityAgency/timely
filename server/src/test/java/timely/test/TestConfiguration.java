package timely.test;

import timely.common.configuration.SecurityProperties;

public class TestConfiguration {

    public static final int WAIT_SECONDS = 2;

    public static SecurityProperties anonymousSecurity() {
        SecurityProperties securityProperties = new SecurityProperties();
        securityProperties.setAllowAnonymousHttpAccess(true);
        securityProperties.setAllowAnonymousWsAccess(true);
        return securityProperties;
    }

    public static SecurityProperties requireUserSecurity() {
        return new SecurityProperties();
    }
}
