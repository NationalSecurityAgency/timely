package timely.balancer.configuration;

import javax.validation.Valid;

import org.springframework.boot.context.properties.NestedConfigurationProperty;
import timely.configuration.ClientSsl;
import timely.configuration.Security;

public class BalancerSecurity extends Security {

    @Valid
    @NestedConfigurationProperty
    private ClientSsl clientSsl = new ClientSsl();

    public ClientSsl getClientSsl() {
        return clientSsl;
    }
}
