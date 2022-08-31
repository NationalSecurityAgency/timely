package timely.auth;

import static timely.auth.TimelyUser.ANONYMOUS_USER;

import java.io.Serializable;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import com.fasterxml.jackson.annotation.JsonIgnore;

import timely.auth.TimelyUser.UserType;
import timely.auth.util.DnUtils;

/**
 * A {@link Principal} that represents a set of proxied {@link TimelyUser}s. For example, this proxied user could represent a GUI server acting on behalf of a
 * user. The GUI server user represents the entity that made the call to us and the other proxied user would be the actual end user.
 */
@XmlRootElement
@XmlType(factoryMethod = "anonymousPrincipal", propOrder = {"name", "proxiedUsers", "creationTime"})
@XmlAccessorType(XmlAccessType.NONE)
public class TimelyPrincipal implements Principal, Serializable, UserDetails {

    private static final long serialVersionUID = 1L;
    private final String username;
    private final TimelyUser primaryUser;
    @XmlElement
    private final ArrayList<TimelyUser> proxiedUsers = new ArrayList<>();
    @XmlElement
    private final long creationTime;

    /**
     * This constructor should not be used. It is here to allow JAX-B mapping and CDI proxying of this class.
     */
    public TimelyPrincipal() {
        this(ANONYMOUS_USER);
    }

    public TimelyPrincipal(String username) {
        this.creationTime = System.currentTimeMillis();
        this.primaryUser = ANONYMOUS_USER;
        this.proxiedUsers.add(ANONYMOUS_USER);
        this.username = username;
    }

    public TimelyPrincipal(TimelyUser timelyUser) {
        this.creationTime = System.currentTimeMillis();
        this.primaryUser = timelyUser;
        this.proxiedUsers.add(timelyUser);
        this.username = timelyUser.getName();
    }

    public TimelyPrincipal(Collection<TimelyUser> proxiedUsers) {
        this(proxiedUsers, System.currentTimeMillis());
    }

    public TimelyPrincipal(Collection<? extends TimelyUser> proxiedUsers, long creationTime) {
        this.proxiedUsers.addAll(proxiedUsers);
        this.creationTime = creationTime;
        this.primaryUser = TimelyPrincipal.findPrimaryUser(this.proxiedUsers);
        this.username = TimelyPrincipal.orderProxiedUsers(this.proxiedUsers).stream().map(TimelyUser::getName).collect(Collectors.joining(" -> "));
    }

    /**
     * Gets the {@link TimelyUser} that represents the primary user in this TimelyPrincipal. If there is only one TimelyUser, then it is the primaryUser. If
     * there is more than one TimelyUser, then the first (and presumably only) TimelyUser whose {@link TimelyUser#getUserType()} is {@link UserType#USER} is the
     * primary user. If no such TimelyUser is present, then the first principal in the list is returned as the primary user. This will be the first entity in
     * the X-ProxiedEntitiesChain which should be the server that originated the request.
     */
    static protected TimelyUser findPrimaryUser(List<TimelyUser> timelyUsers) {
        if (timelyUsers.isEmpty()) {
            return null;
        } else {
            return timelyUsers.get(findPrimaryUserPosition(timelyUsers));
        }
    }

    static protected int findPrimaryUserPosition(List<TimelyUser> datawaveUsers) {
        if (datawaveUsers.isEmpty()) {
            return -1;
        } else {
            for (int x = 0; x < datawaveUsers.size(); x++) {
                if (datawaveUsers.get(x).getUserType().equals(UserType.USER)) {
                    return x;
                }
            }
            return 0;
        }
    }

    /*
     * The purpose here is to return a List of TimelyUsers where the original caller is first followed by any entities in X-ProxiedEntitiesChain in the order
     * that they were traversed and ending with the entity that made the final call. The List that is passed is not modified. This method makes the following
     * assumptions about the List that is passed to ths method: 1) The first element is the one that made the final call 2) Additional elements (if any) are
     * from X-ProxiedEntitiesChain in chronological order of the calls
     */
    static protected List<TimelyUser> orderProxiedUsers(List<TimelyUser> timelyUsers) {
        List<TimelyUser> users = new ArrayList<>();
        int position = TimelyPrincipal.findPrimaryUserPosition(timelyUsers);
        if (position >= 0) {
            users.add(timelyUsers.get(position));
            if (timelyUsers.size() > 1) {
                timelyUsers.stream().limit(position).forEach(u -> users.add(u));
                timelyUsers.stream().skip(position + 1).forEach(u -> users.add(u));
            }
        }
        return users;
    }

    public Collection<TimelyUser> getProxiedUsers() {
        return Collections.unmodifiableCollection(this.proxiedUsers);
    }

    public TimelyUser getPrimaryUser() {
        return primaryUser;
    }

    public Collection<? extends Collection<String>> getAuthorizations() {
        // @formatter:off
        return Collections.unmodifiableCollection(
                TimelyPrincipal.orderProxiedUsers(this.proxiedUsers).stream()
                        .map(TimelyUser::getAuths)
                        .collect(Collectors.toList()));
        // @formatter:on
    }

    public String[] getDNs() {
        // @formatter:off
        return TimelyPrincipal.orderProxiedUsers(this.proxiedUsers).stream()
                .map(TimelyUser::getDn)
                .map(SubjectIssuerDNPair::subjectDN)
                .toArray(String[]::new);
        // @formatter:on
    }

    public long getCreationTime() {
        return this.creationTime;
    }

    @Override
    @XmlElement
    public String getName() {
        return this.username;
    }

    public String getShortName() {
        return DnUtils.getShortName(getPrimaryUser().getName());
    }

    public SubjectIssuerDNPair getUserDN() {
        return getPrimaryUser().getDn();
    }

    public List<String> getProxyServers() {

        // @formatter:off
        List<String> proxyServers = orderProxiedUsers(this.proxiedUsers).stream()
                .filter(u -> u.getUserType() == UserType.SERVER)
                .filter(u -> !u.equals(this.primaryUser))
                .map(TimelyUser::getDn)
                .map(SubjectIssuerDNPair::subjectDN)
                .collect(Collectors.toList());
        // @formatter:on
        return proxyServers.isEmpty() ? null : proxyServers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TimelyPrincipal that = (TimelyPrincipal) o;

        if (!username.equals(that.username))
            return false;
        return proxiedUsers.equals(that.proxiedUsers);
    }

    @Override
    public int hashCode() {
        int result = username.hashCode();
        result = 31 * result + proxiedUsers.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TimelyPrincipal{" + "name='" + username + "'" + ", proxiedUsers=" + TimelyPrincipal.orderProxiedUsers(proxiedUsers) + "}";
    }

    public static TimelyPrincipal anonymousPrincipal() {
        return new TimelyPrincipal("ANONYMOUS");
    }

    public String getAuthorizationsString() {
        return orderProxiedUsers(this.proxiedUsers).stream().map(TimelyUser::getAuths).map(a -> a.toString()).collect(Collectors.joining(" -> ", "[", "]"));
    }

    @Override
    @JsonIgnore
    public Collection<? extends GrantedAuthority> getAuthorities() {
        final Collection<SimpleGrantedAuthority> auths = new ArrayList<>();
        for (String auth : getPrimaryUser().getAuths()) {
            auths.add(new SimpleGrantedAuthority(auth));
        }
        return auths;
    }

    @Override
    @JsonIgnore
    public String getPassword() {
        return "";
    }

    @Override
    @JsonIgnore
    public String getUsername() {
        return getPrimaryUser().getName();
    }

    @Override
    @JsonIgnore
    public boolean isAccountNonExpired() {
        return true;
    }

    @Override
    @JsonIgnore
    public boolean isAccountNonLocked() {
        return true;
    }

    @Override
    @JsonIgnore
    public boolean isCredentialsNonExpired() {
        return true;
    }

    @Override
    @JsonIgnore
    public boolean isEnabled() {
        return true;
    }
}
