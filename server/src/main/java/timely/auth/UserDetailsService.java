package timely.auth;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

public class UserDetailsService
        implements
        org.springframework.security.core.userdetails.AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> {

    private HashMap<String, List<String>> users;

    public Map<String, List<String>> getUsers() {
        return users;
    }

    public void setUsers(LinkedHashMap<String, List<String>> users) {
        this.users = new HashMap<>(users);
    }

    @Override
    public UserDetails loadUserDetails(final PreAuthenticatedAuthenticationToken token)
            throws UsernameNotFoundException {
        // Determine if the user is allowed to access the system, if not throw
        // UsernameNotFoundException
        String username = token.getName().toString();
        if (!users.containsKey(username)) {
            throw new UsernameNotFoundException(username + " not configured.");
        }
        // If allowed, populate the user details object with the authorities for
        // the user.
        return new UserDetails() {

            private static final long serialVersionUID = 1L;

            @Override
            public Collection<SimpleGrantedAuthority> getAuthorities() {
                final Collection<SimpleGrantedAuthority> auths = new ArrayList<>();
                users.get(username).forEach(a -> {
                    auths.add(new SimpleGrantedAuthority(a));
                });
                return auths;
            }

            @Override
            public String getPassword() {
                return "";
            }

            @Override
            public String getUsername() {
                return username;
            }

            @Override
            public boolean isAccountNonExpired() {
                return true;
            }

            @Override
            public boolean isAccountNonLocked() {
                return true;
            }

            @Override
            public boolean isCredentialsNonExpired() {
                return true;
            }

            @Override
            public boolean isEnabled() {
                return true;
            }

        };
    }

}
