package timely.auth;

import timely.auth.util.DnUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static timely.auth.TimelyUser.ANONYMOUS_USER;

/**
 * A {@link Principal} that represents a set of proxied {@link TimelyUser}s. For example, this proxied user could represent a GUI server acting on behalf of a
 * user. The GUI server user represents the entity that made the call to us and the other proxied user would be the actual end user.
 */
@XmlRootElement
@XmlType(factoryMethod = "anonymousPrincipal", propOrder = {"name", "proxiedUsers", "creationTime"})
@XmlAccessorType(XmlAccessType.NONE)
public class TimelyPrincipal implements Principal, Serializable {
    private final String username;
    private final TimelyUser primaryUser;
    @XmlElement
    private final Set<TimelyUser> proxiedUsers = new LinkedHashSet<>();
    @XmlElement
    private final long creationTime;

    /**
     * This constructor should not be used. It is here to allow JAX-B mapping and CDI proxying of this class.
     */
    public TimelyPrincipal() {
        this(ANONYMOUS_USER);
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

    public TimelyPrincipal(Collection<TimelyUser> proxiedUsers, long creationTime) {
        this.creationTime = creationTime;
        TimelyUser first = proxiedUsers.stream().findFirst().orElse(null);
        this.primaryUser = proxiedUsers.stream().filter(u -> u.getUserType() == TimelyUser.UserType.USER).findFirst()
                .orElse(first);
        this.proxiedUsers.add(first);
        this.proxiedUsers.addAll(proxiedUsers.stream().filter(u -> !u.equals(primaryUser)).collect(Collectors.toList()));
        this.username = this.proxiedUsers.stream().map(TimelyUser::getName).collect(Collectors.joining(" -> "));
    }

    public Set<TimelyUser> getProxiedUsers() {
        return Collections.unmodifiableSet(proxiedUsers);
    }

    public TimelyUser getPrimaryUser() {
        return primaryUser;
    }

    public Collection<? extends Collection<String>> getAuthorizations() {
        return Collections.unmodifiableCollection(proxiedUsers.stream().map(TimelyUser::getAuths).collect(Collectors.toList()));
    }

    public String[] getDNs() {
        return proxiedUsers.stream().map(TimelyUser::getDn).map(SubjectIssuerDNPair::subjectDN).toArray(String[]::new);
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    @XmlElement
    public String getName() {
        return username;
    }

    public String getShortName() {
        return DnUtils.getShortName(getPrimaryUser().getName());
    }

    public SubjectIssuerDNPair getUserDN() {
        return getPrimaryUser().getDn();
    }

    public Set<String> getProxyServers() {

        // @formatter:off
        Set<String> proxyServers = getProxiedUsers().stream()
                .filter(u -> u.getUserType() == TimelyUser.UserType.SERVER)
                .map(TimelyUser::getDn)
                .map(SubjectIssuerDNPair::subjectDN)
                .collect(Collectors.toSet());
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
        return "TimelyPrincipal{" + "name='" + username + "'" + ", proxiedUsers=" + proxiedUsers + "}";
    }

    public static TimelyPrincipal anonymousPrincipal() {
        return new TimelyPrincipal(ANONYMOUS_USER);
    }

    public String getAuthorizationsString() {
        return proxiedUsers.stream().map(TimelyUser::getAuths).map(a -> a.toString())
                .collect(Collectors.joining(" -> ", "[", "]"));
    }
}
