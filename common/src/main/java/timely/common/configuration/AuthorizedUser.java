package timely.common.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

public class AuthorizedUser {

    private String subjectDn;
    private String issuerDn;
    private String auths;
    private String roles;
    private String userType;

    public void setSubjectDn(String subjectDn) {
        this.subjectDn = subjectDn;
    }

    public String getSubjectDn() {
        return subjectDn;
    }

    public void setIssuerDn(String issuerDn) {
        this.issuerDn = issuerDn;
    }

    public String getIssuerDn() {
        return issuerDn;
    }

    public void setAuths(String auths) {
        this.auths = auths;
    }

    public String getAuths() {
        return auths;
    }

    public Collection<String> getAuthsCollection() {
        if (this.auths == null) {
            return new ArrayList<>();
        } else {
            return Arrays.stream(StringUtils.split(this.auths, ',')).map(String::trim).collect(Collectors.toSet());
        }
    }

    public void setRoles(String roles) {
        this.roles = roles;
    }

    public String getRoles() {
        return roles;
    }

    public Collection<String> getRolesCollection() {
        if (this.roles == null) {
            return new ArrayList<>();
        } else {
            return Arrays.stream(StringUtils.split(this.roles, ',')).map(String::trim).collect(Collectors.toSet());
        }
    }

    public void setUserType(String userType) {
        this.userType = userType;
    }

    public String getUserType() {
        return userType;
    }
}
