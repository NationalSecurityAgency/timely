package timely.netty.http.auth;

import java.util.Collection;

import org.springframework.security.core.userdetails.UserDetails;

public interface TimelyUserDetails extends UserDetails {

    Collection<String> getRoles();
}
