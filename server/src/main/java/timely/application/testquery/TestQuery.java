package timely.application.testquery;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

import timely.common.configuration.TimelyCommonConfiguration;

@SpringBootApplication(scanBasePackageClasses = {TimelyCommonConfiguration.class, TestQuery.class}, exclude = {ErrorMvcAutoConfiguration.class})
public class TestQuery {

    public static void main(String[] args) {
        new SpringApplicationBuilder(TestQuery.class).main(TestQuery.class).web(WebApplicationType.NONE).run(args);
    }
}
