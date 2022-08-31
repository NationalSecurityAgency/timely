package timely.common.component;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;
import org.springframework.stereotype.Component;

@Component
@Primary
public class TestTimelyAuthenticationManager extends ProviderManager {

    @Autowired
    public TestTimelyAuthenticationManager(@Qualifier("file") PreAuthenticatedAuthenticationProvider file) {
        super(Collections.singletonList(file));
    }
}
