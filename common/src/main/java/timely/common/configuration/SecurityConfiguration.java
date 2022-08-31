package timely.common.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;
import org.springframework.web.reactive.function.client.WebClient;

import timely.auth.FileUserDetailsService;
import timely.auth.RemoteAuthenticationProvider;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyAuthenticationManager;
import timely.auth.TimelyUser;

@Configuration
@EnableConfigurationProperties({SecurityProperties.class})
public class SecurityConfiguration {

    @Bean
    @ConditionalOnExpression("T(org.apache.commons.lang3.StringUtils).isNotEmpty('${timely.security.authorization-url:}')")
    public TimelyAuthenticationManager remoteAuthenticationManager(RemoteAuthenticationProvider remoteAuthenticationProvider) {
        return new TimelyAuthenticationManager(Arrays.asList(remoteAuthenticationProvider));
    }

    @Bean
    @ConditionalOnExpression("T(org.apache.commons.lang3.StringUtils).isNotEmpty('${timely.security.authorization-url:}')")
    public RemoteAuthenticationProvider remoteAuthenticationProvider(SecurityProperties securityProperties, WebClient.Builder builder) {
        return new RemoteAuthenticationProvider(builder, securityProperties);
    }

    @Bean
    @Qualifier("file")
    @ConditionalOnExpression("T(org.apache.commons.lang3.StringUtils).isEmpty('${timely.security.authorization-url:}')")
    public PreAuthenticatedAuthenticationProvider fileAuthenticationProvider(FileUserDetailsService fileUserDetailsService) {
        PreAuthenticatedAuthenticationProvider authenticationProvider = new PreAuthenticatedAuthenticationProvider();
        authenticationProvider.setPreAuthenticatedUserDetailsService(fileUserDetailsService);
        return authenticationProvider;
    }

    @Bean
    @ConditionalOnExpression("T(org.apache.commons.lang3.StringUtils).isEmpty('${timely.security.authorization-url:}')")
    public TimelyAuthenticationManager fileAuthenticationManager(@Qualifier("file") PreAuthenticatedAuthenticationProvider fileAuthenticationProvider) {
        return new TimelyAuthenticationManager(Arrays.asList(fileAuthenticationProvider));
    }

    @Bean
    public FileUserDetailsService fileUserDetailsService(SecurityProperties securityProperties) {
        List<TimelyUser> timelyUsers = new ArrayList<>();
        List<AuthorizedUser> users = securityProperties.getAuthorizedUsers();
        for (AuthorizedUser user : users) {
            TimelyUser timelyUser = new TimelyUser(SubjectIssuerDNPair.of(user.getSubjectDn(), user.getIssuerDn()),
                            TimelyUser.UserType.valueOf(user.getUserType()), "", user.getAuthsCollection(), user.getRolesCollection(), null,
                            System.currentTimeMillis(), -1L);
            timelyUsers.add(timelyUser);
        }
        FileUserDetailsService fileUserDetailsService = new FileUserDetailsService();
        fileUserDetailsService.setUsers(timelyUsers);
        return fileUserDetailsService;
    }
}
