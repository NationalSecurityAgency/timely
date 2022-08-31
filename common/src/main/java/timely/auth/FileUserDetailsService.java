package timely.auth;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

public class FileUserDetailsService
                implements org.springframework.security.core.userdetails.AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> {

    private HashMap<String,TimelyUser> users;

    public Map<String,TimelyUser> getUsers() {
        return users;
    }

    public void setUsers(List<TimelyUser> users) {
        this.users = new HashMap<>();
        users.forEach(u -> this.users.put(u.getName(), u));
    }

    @Override
    public UserDetails loadUserDetails(final PreAuthenticatedAuthenticationToken token) throws UsernameNotFoundException {
        // Determine if the user is allowed to access the system, if not throw
        // UsernameNotFoundException
        String username = token.getName();
        TimelyUser timelyUser = users.get(username);
        if (timelyUser == null) {
            throw new UsernameNotFoundException(username + " not configured.");
        }
        return new TimelyPrincipal(timelyUser);
    }
}
