package timely.common.component;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;
import org.springframework.stereotype.Component;

@Component
public class TimelyAuthenticationManager extends ProviderManager {

    @Autowired
    public TimelyAuthenticationManager(RemoteAuthenticationProvider remoteAuthenticationProvider,
                    PreAuthenticatedAuthenticationProvider fileAuthenticationProvider) {
        super(Arrays.asList(remoteAuthenticationProvider, fileAuthenticationProvider));
    }
}
