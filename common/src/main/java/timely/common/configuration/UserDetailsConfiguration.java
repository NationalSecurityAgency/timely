package timely.common.configuration;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;

import timely.auth.FileUserDetailsService;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyUser;

@Configuration
@EnableConfigurationProperties({SecurityProperties.class})
public class UserDetailsConfiguration {

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

    @Bean
    @Qualifier("file")
    PreAuthenticatedAuthenticationProvider fileAuthenticationProvider(FileUserDetailsService fileUserDetailsService) {
        PreAuthenticatedAuthenticationProvider authenticationProvider = new PreAuthenticatedAuthenticationProvider();
        authenticationProvider.setPreAuthenticatedUserDetailsService(fileUserDetailsService);
        return authenticationProvider;
    }
}
