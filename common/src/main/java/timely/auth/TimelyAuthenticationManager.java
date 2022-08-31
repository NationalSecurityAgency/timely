package timely.auth;

import java.util.List;

import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.ProviderManager;

public class TimelyAuthenticationManager extends ProviderManager {

    public TimelyAuthenticationManager(List<AuthenticationProvider> providers) {
        super(providers);
    }
}
